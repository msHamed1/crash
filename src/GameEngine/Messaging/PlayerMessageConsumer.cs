using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Domain.Contracts;
using Crash.Domain.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace GameEngine.Messaging;

public sealed class PlayerMessageConsumer(
    PlayerBrokerOptions brokerOptions,
    GameEngineOptions gameEngineOptions,
    ILogger<PlayerMessageConsumer> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                Consume(stoppingToken);
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "Player message consumer failed. Retrying in 5s.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private void Consume(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = brokerOptions.HostName,
            Port = brokerOptions.Port,
            UserName = brokerOptions.UserName,
            Password = brokerOptions.Password,
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: brokerOptions.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false);

        channel.BasicQos(prefetchSize: 0, prefetchCount: 25, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, args) =>
        {
            try
            {
                var json = Encoding.UTF8.GetString(args.Body.ToArray());
                var message = JsonSerializer.Deserialize<PlayerMessageEnvelope>(json, JsonOptions)
                    ?? throw new InvalidOperationException("Player message body is empty.");

                Console.WriteLine(
                    "GameEngine {0} received {1} for table {2}, player {3}, message {4}",
                    gameEngineOptions.EngineId,
                    message.Type,
                    message.TableId,
                    message.PlayerId,
                    message.MessageId);

                channel.BasicAck(args.DeliveryTag, multiple: false);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed to process player message.");
                channel.BasicNack(args.DeliveryTag, multiple: false, requeue: true);
            }

            await Task.CompletedTask;
        };

        foreach (var tableId in gameEngineOptions.TableIds.Where(tableId => !string.IsNullOrWhiteSpace(tableId)))
        {
            var normalizedTableId = tableId.Trim();
            var queueName = GetTableQueueName(normalizedTableId);

            channel.QueueDeclare(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false);

            // Each engine consumes only the durable table queues it owns.
            channel.QueueBind(
                queue: queueName,
                exchange: brokerOptions.ExchangeName,
                routingKey: normalizedTableId);

            channel.BasicConsume(
                queue: queueName,
                autoAck: false,
                consumer: consumer);
        }

        logger.LogInformation(
            "Game engine {EngineId} is consuming player messages for tables: {TableIds}.",
            gameEngineOptions.EngineId,
            string.Join(", ", gameEngineOptions.TableIds));

        WaitHandle.WaitAny([stoppingToken.WaitHandle]);
    }

    private static string GetTableQueueName(string tableId)
    {
        return $"table.{tableId}.player-messages";
    }
}
