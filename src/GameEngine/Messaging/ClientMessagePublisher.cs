using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Domain.Contracts.ServerMessages;
using Crash.Domain.Options;
using RabbitMQ.Client;

namespace GameEngine.Messaging;



public interface IClientMessagePublisher
{
    Task PublishAsync(ToClient message, CancellationToken ct);
}
public class ClientMessagePublisher:IClientMessagePublisher,IDisposable
{
    
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    
    
    
    private readonly FanoutOptions options;
    private IConnection? connection;
    private IModel? channel;
    private readonly object publishLock = new();

    public ClientMessagePublisher(FanoutOptions options)
    {
        this.options = options;
    }
    
    private static IConnection CreateConnection(FanoutOptions options)
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
    private IModel GetChannel()
    {
        if (connection?.IsOpen == true && channel?.IsOpen == true)
        {
            return channel;
        }

        channel?.Dispose();
        connection?.Dispose();

        connection = CreateConnection(options);
        channel = connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: options.ExchangeName,
            type: "direct",
            durable: true,
            autoDelete: false
        );
        channel.QueueDeclare(
            queue: options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false
        );
        channel.QueueBind(
            queue: options.QueueName,
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey);

        return channel;
    }

    public Task PublishAsync(ToClient message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var json = JsonSerializer.Serialize(message, message.GetType(), JsonOptions);
        var body = Encoding.UTF8.GetBytes(json);
        lock (publishLock)
        {
            var rabbitChannel = GetChannel();
            var properties = rabbitChannel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";
            properties.MessageId = message.MessageId;
            properties.Type = message.MessageType;
            properties.Timestamp=new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

            // Use message type as the routing key so the message lands in that table's durable queue.
            rabbitChannel.BasicPublish(
                exchange: options.ExchangeName,
                routingKey: options.RoutingKey,
                mandatory: false,
                basicProperties: properties,
                body: body);
        }
        
        return Task.CompletedTask;

    }

    public void Dispose()
    {
        channel?.Dispose();
        connection?.Dispose();
    }
}
