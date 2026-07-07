using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Domain.Contracts.BetMessages;
using Crash.Domain.Contracts.Consumer;
using Crash.Domain.Contracts.PlayerMessages;
using Crash.Domain.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace GameEngine.Messaging;

public sealed class PlayerMessageConsumer   : BackgroundService
{

    private readonly GameEngineOptions _gameEngineOptions;

    private readonly PlayerBrokerOptions _brokerOptions;

    private readonly ILogger<PlayerMessageConsumer> _logger;

    public PlayerMessageConsumer( GameEngineOptions _gameEngineOptions,PlayerBrokerOptions _brokerOptions,ILogger<PlayerMessageConsumer> _logger)
    {
        this._brokerOptions = _brokerOptions;
        this._logger=_logger;
        this._gameEngineOptions=_gameEngineOptions;
        
    }
    

    private static readonly TimeSpan OwnershipReconcileInterval = TimeSpan.FromSeconds(1);

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };

    private readonly object _channelLock = new();
    private readonly Dictionary<long, string> _consumerTagsPerTable = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Player message consumer failed. Retrying in 5s.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _brokerOptions.HostName,
            Port = _brokerOptions.Port,
            UserName = _brokerOptions.UserName,
            Password = _brokerOptions.Password,
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: _brokerOptions.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false);

        channel.BasicQos(prefetchSize: 0, prefetchCount: 25, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, args) =>
        {
            MessageHeader? header = null;

            try
            {
                var json = Encoding.UTF8.GetString(args.Body.ToArray());
                header = ReadMessageHeader(json);


                var tableId = header.TableId;
                if (!_gameEngineOptions.tokensPerTable.ContainsKey(tableId))
                {
                    // A cancelled consumer may still receive an in-flight delivery. Requeue it so the
                    // current owner gets the message instead of this stale engine acknowledging it.
                    _logger.LogInformation(
                        "Requeueing player message {MessageId} for table {TableId} because this engine is no longer the owner.",
                        header.MessageType,
                        tableId);

                    SafeNack(channel, args.DeliveryTag, requeue: true);
                    return;
                }

                _logger.LogInformation(
                    "GameEngine {EngineId} received {MessageType} for table {TableId}, message {MessageId}.",
                    _gameEngineOptions.EngineId,
                    header.MessageType,
                    header.TableId,
                    header.MessageId);

                await DispatchMessageAsync(header.MessageType, json, stoppingToken);

                SafeAck(channel, args.DeliveryTag);
                
                
                // Send Event to Game Engine
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Failed to process message {MessageId}.",
                    header?.MessageId);
                SafeNack(channel, args.DeliveryTag, requeue: true);
            }

            await Task.CompletedTask;
        };

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ReconcileTableConsumers(channel, consumer);
                await Task.Delay(OwnershipReconcileInterval, stoppingToken);
            }
        }
        finally
        {
            CancelAllConsumers(channel);
        }
    }

    private void ReconcileTableConsumers(IModel channel, AsyncEventingBasicConsumer consumer)
    {
        var ownedTableIds = _gameEngineOptions.tokensPerTable.Keys.ToHashSet();

        foreach (var tableId in ownedTableIds)
        {
            if (_consumerTagsPerTable.ContainsKey(tableId))
            {
                continue;
            }

            StartConsumingTable(channel, consumer, tableId);
        }

        foreach (var tableId in _consumerTagsPerTable.Keys.ToArray())
        {
            if (ownedTableIds.Contains(tableId))
            {
                continue;
            }

            StopConsumingTable(channel, tableId);
        }
    }

    private void StartConsumingTable(IModel channel, AsyncEventingBasicConsumer consumer, long tableId)
    {
        var normalizedTableId = tableId.ToString();
        var queueName = GetTableQueueName(normalizedTableId);

        lock (_channelLock)
        {
            channel.QueueDeclare(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false);

            // The queue is table-owned, not engine-owned. When ownership moves, the previous
            // engine cancels its consumer and the new owner consumes the same durable queue.
            channel.QueueBind(
                queue: queueName,
                exchange: _brokerOptions.ExchangeName,
                routingKey: normalizedTableId);

            var consumerTag = channel.BasicConsume(
                queue: queueName,
                autoAck: false,
                consumer: consumer);

            _consumerTagsPerTable[tableId] = consumerTag;
        }

        _logger.LogInformation(
            "Game engine {EngineId} started consuming player messages for table {TableId}.",
            _gameEngineOptions.EngineId,
            tableId);
    }

    private void StopConsumingTable(IModel channel, long tableId)
    {
        if (!_consumerTagsPerTable.TryGetValue(tableId, out var consumerTag))
        {
            return;
        }

        lock (_channelLock)
        {
            channel.BasicCancel(consumerTag);
            _consumerTagsPerTable.Remove(tableId);
        }

        _logger.LogWarning(
            "Game engine {EngineId} stopped consuming player messages for table id {TableId} because ownership was lost.",
            _gameEngineOptions.EngineId,
            tableId);
    }

    private void CancelAllConsumers(IModel channel)
    {
        foreach (var tableId in _consumerTagsPerTable.Keys.ToArray())
        {
            StopConsumingTable(channel, tableId);
        }
    }

    private void SafeAck(IModel channel, ulong deliveryTag)
    {
        lock (_channelLock)
        {
            channel.BasicAck(deliveryTag, multiple: false);
        }
    }

    private void SafeNack(IModel channel, ulong deliveryTag, bool requeue)
    {
        lock (_channelLock)
        {
            channel.BasicNack(deliveryTag, multiple: false, requeue: requeue);
        }
    }

    private static string GetTableQueueName(string tableId)
    {
        return $"table.{tableId}.player-messages";
    }
    
    private static MessageHeader ReadMessageHeader(string json)
    {
        return JsonSerializer.Deserialize<MessageHeader>(json, JsonOptions)
               ?? throw new InvalidOperationException("Message body is empty.");
    }
    
    private async Task DispatchMessageAsync(
        string messageType,
        string json,
        CancellationToken ct)
    {
        switch (messageType)
        {
            case "place-bet":
            {
                var message = JsonSerializer.Deserialize<PlaceBetReqEvent>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid place-bet message.");

              //  await _gameEngine.PlaceBetAsync(message, ct);
              
              _logger.LogInformation("Player {PlayerId} place bet",message.Data.PlayerId);

                break;
            }

            case "cash-out":
            {
                var message = JsonSerializer.Deserialize<CashoutBetEvent>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid cash-out message.");

              //  await _gameEngine.CashOutAsync(message, ct);
              _logger.LogInformation("Player {PlayerId} cash out",message.Data.PlayerId);

                break;
            }

            case "player-joined":
            {
                var message = JsonSerializer.Deserialize<PlayerJoinedEvent>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid player-joined message.");

             //   await _gameEngine.PlayerJoinedAsync(message, ct);
             _logger.LogInformation("Player {PlayerId} Joined the table {TableId}",message.Data.PlayerId,message.TableId);
                break;
            }

            case "player-left":
            {
                var message = JsonSerializer.Deserialize<PlayerLeftEvent>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid player-left message.");

                _logger.LogInformation("Player {PlayerId} Left the table {TableId}",message.Data.PlayerId,message.TableId);
             //   await _gameEngine.PlayerLeftAsync(message, ct);
                break;
            }

            default:
                throw new InvalidOperationException($"Unsupported message type: {messageType}");
        }
    }
}
