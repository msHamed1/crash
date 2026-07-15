using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Contracts.Messaging.Common;
using Crash.Domain.Options;
using RabbitMQ.Client;
 
namespace RealtimeGateway.Messaging;

public interface IPlayerMessagePublisher
{
    Task PublishAsync<T>(T message,PublisherOptions configs, CancellationToken cancellationToken);
}

public sealed record PublisherOptions
{
    public required string MessageId {get; init;}
    public required string Type {get; init;}
    public AmqpTimestamp Timestamp {get; init;}
    public required string TableId {get; init;}
}
public sealed class PlayerMessagePublisher : IPlayerMessagePublisher, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    private readonly PlayerBrokerOptions options;
    private readonly IConnection connection;
    private readonly IModel channel;
    private readonly object publishLock = new();

    public PlayerMessagePublisher(PlayerBrokerOptions options)
    {
        this.options = options;
        connection = CreateConnection(options);
        channel = connection.CreateModel();
    }
     public Task PublishAsync<T>(T message,PublisherOptions configs, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, JsonOptions));

        lock (publishLock)
        {
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";
            properties.MessageId = configs.MessageId;
            properties.Type = configs.Type;
            properties.Timestamp = configs.Timestamp;// new AmqpTimestamp(message.ReceivedAt.ToUnixTimeSeconds());

            channel.ExchangeDeclare(
                exchange: options.ExchangeName,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false);
            channel.QueueDeclare(
                queue: GetTableQueueName(configs.TableId),
                durable: true,
                exclusive: false,
                autoDelete: false);
            channel.QueueBind(
                queue: GetTableQueueName(configs.TableId),
                exchange: options.ExchangeName,
                routingKey: configs.TableId);

            // Use table id as the routing key so the message lands in that table's durable queue.
            channel.BasicPublish(
                exchange: options.ExchangeName,
                routingKey: configs.TableId,
                mandatory: false,
                basicProperties: properties,
                body: body);
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        channel.Dispose();
        connection.Dispose();
    }

    private static IConnection CreateConnection(PlayerBrokerOptions options)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.HostName,
            Port = options.Port,
            UserName = options.UserName,
            Password = options.Password,
            DispatchConsumersAsync = true
        };

        return factory.CreateConnection();
    }

    private static string GetTableQueueName(string tableId)
    {
        return $"table.{tableId}.player-messages";
    }
}
