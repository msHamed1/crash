using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Contracts.Messaging.EngineToGateway.Bets;
using Crash.Contracts.Messaging.EngineToGateway.Rounds;
using Crash.Domain.Options;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RealtimeGateway.Hubs;
using RealtimeGateway.Contracts.WebSockets.Outbound;

namespace RealtimeGateway.Messaging;

public class ClientMessagesConsumer(
    FanoutOptions options,
    IHubContext<PlayerHub> hubContext,

    ILogger<ClientMessagesConsumer> logger) :BackgroundService
{
    
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    
    private readonly object _channelLock = new();
    protected async override Task ExecuteAsync(CancellationToken stoppingToken)
    {

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                Consume(stoppingToken);

                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);

            }
            catch (Exception e)
            {
                logger.LogWarning(e, "RealTime message consumer failed. Retrying in 5s.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            
        }

        return;
    }
    
   

    private void Consume(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.HostName,
            UserName = options.UserName,
            Password = options.Password,
            Port = options.Port,
            DispatchConsumersAsync = true
        };
        
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            exchange: options.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false);
        channel.BasicQos(prefetchSize: 0, prefetchCount: 25, global: false);
        var consumer = new AsyncEventingBasicConsumer(channel);
        
        consumer.Received +=async (_, args) =>
        {
            try
            {
                var json = Encoding.UTF8.GetString(args.Body.ToArray());
                await DispatchToClientAsync(json, ct);
               
                
                SafeAck(channel, args.DeliveryTag);

            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to process player message.");
                SafeNack(channel, args.DeliveryTag, requeue: true);            }
            await Task.CompletedTask;

        };

        channel.QueueDeclare(
            queue: options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false);
        
        channel.QueueBind(
            queue: options.QueueName,
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey);

        channel.BasicConsume(
            queue: options.QueueName,
            autoAck: false,
            consumer: consumer);
        
        logger.LogInformation(
            "DbWorker is consuming DbMessages messages for tables: DbWorkers."
        );
        WaitHandle.WaitAny([ct.WaitHandle]);


    }
    
    private async Task DispatchToClientAsync(string json, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(json);

        var messageType = doc.RootElement
            .GetProperty("messageType")
            .GetString();

        switch (messageType)
        {
            case "NewRoundInfo":
            {
                var message = JsonSerializer.Deserialize<RoundCreated>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid NewRoundInfo message.");

                var notification = new RoundCreatedNotification(
                    message.MessageType,
                    message.MessageId,
                    message.TableId,
                    message.RoundId,
                    message.CurrentMultiplier,
                    message.StartsAt,
                    message.IsCrashed);

                await hubContext.Clients
                    .Group(message.TableId.ToString())
                    .SendAsync(message.MessageType, notification, ct);

                break;
            }

            case "RoundTick":
            {
                var message = JsonSerializer.Deserialize<RoundUpdated>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid RoundTick message.");

                var notification = new RoundUpdatedNotification(
                    message.MessageType,
                    message.MessageId,
                    message.TableId,
                    message.RoundId,
                    message.CurrentMultiplier,
                    message.TickSequence);

                await hubContext.Clients
                    .Group(message.TableId.ToString())
                    .SendAsync(message.MessageType, notification, ct);

                break;
            }
            
            case "RoundCrashed":
            {
                var message = JsonSerializer.Deserialize<RoundCrashed>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid RoundCrashed message.");

                var notification = new RoundCrashedNotification(
                    message.MessageType,
                    message.MessageId,
                    message.TableId,
                    message.RoundId,
                    message.CurrentMultiplier,
                    message.IsCrashed,
                    message.TickSequence);

                await hubContext.Clients
                    .Group(message.TableId.ToString())
                    .SendAsync(message.MessageType, notification, ct);

                break;
            }

            case "PlayerBetAccepted":
            {
                var message = JsonSerializer.Deserialize<BetAccepted>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid PlayerBetAccepted message.");

                var response = new PlaceBetResponse
                {
                    MessageType = message.MessageType,
                    TableId = message.TableId,
                    MessageId = message.MessageId,
                    PlayerId = message.PlayerId,
                    Accepted = true,
                    UpdatedBalance = message.UpdatedBalance,
                    Bet = message.Bet
                };

                // The payload contains a private balance, so it must not be broadcast to the table.
                await hubContext.Clients
                    .Group($"player:{message.PlayerId}")
                    .SendAsync(message.MessageType, response, ct);
                break;
            }

            case "PlayerBetRejected":
            {
                var message = JsonSerializer.Deserialize<BetRejected>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid PlayerBetRejected message.");

                var response = new PlaceBetResponse
                {
                    MessageType = message.MessageType,
                    TableId = message.TableId,
                    MessageId = message.MessageId,
                    PlayerId = message.PlayerId,
                    Accepted = false,
                    UpdatedBalance = message.UpdatedBalance,
                    Code = message.Code,
                    Reason = message.Reason
                };

                await hubContext.Clients
                    .Group($"player:{message.PlayerId}")
                    .SendAsync(message.MessageType, response, ct);
                break;
            }

            case "BetCashedOut":
            {
                var message = JsonSerializer.Deserialize<BetCashedOut>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid BetCashedOut message.");

                var notification = new BetCashedOutNotification
                {
                    MessageType = message.MessageType,
                    MessageId = message.MessageId,
                    BetId = message.BetId,
                    RoundId = message.RoundId,
                    CashoutMultiplier = message.CashoutMultiplier,
                    PayoutAmount = message.PayoutAmount,
                    NetResultAmount = message.NetResultAmount,
                    UpdatedBalance = message.UpdatedBalance,
                    CashedOutAt = message.CashedOutAt
                };

                // Cashout details and balance are private to the affected player.
                await hubContext.Clients
                    .Group($"player:{message.PlayerId}")
                    .SendAsync(message.MessageType, notification, ct);
                break;
            }

            case "CurrentState":
            {
                var message = JsonSerializer.Deserialize<RoundStateSnapshot>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid CurrentState message.");

                var notification = new RoundStateNotification(
                    message.MessageType,
                    message.MessageId,
                    message.TableId,
                    message.RoundId,
                    message.CurrentMultiplier,
                    message.StartsAt,
                    message.IsCrashed);

                await hubContext.Clients
                    .Group($"player:{message.PlayerId}")
                    .SendAsync(message.MessageType, notification, ct);

                break;
            }

            default:
                throw new InvalidOperationException($"Unsupported client message type: {messageType}");
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
}
