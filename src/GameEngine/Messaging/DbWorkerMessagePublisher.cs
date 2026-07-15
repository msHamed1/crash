using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Contracts.Messaging.DbWorkers;
using Crash.Domain.Options;
using RabbitMQ.Client;

namespace GameEngine.Messaging;


public interface IDbWorkerMessagePublisher
{
    Task PublishAsync(DbWorkerMessageEnvelope message, CancellationToken ct);
}
public class DbWorkerMessagePublisher :IDbWorkerMessagePublisher, IDisposable
{
    
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    private readonly DbBrokerOptions options;
    private readonly IConnection connection;
    private readonly IModel channel;
    private readonly object publishLock = new();

    public DbWorkerMessagePublisher(DbBrokerOptions options  )
    {
        this.options = options;
        connection = CreateConnection(options);
        channel = connection.CreateModel();
        
    }
    public Task PublishAsync(DbWorkerMessageEnvelope message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, JsonOptions));

        lock (publishLock)
        {
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";
            properties.MessageId = message.MessageId.ToString();
            properties.Type = message.Type.ToString();
            properties.Timestamp=new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            
            channel.ExchangeDeclare(
                exchange: options.ExchangeName,
                type: "direct",
                durable: true,
                autoDelete:false
                );
            channel.QueueDeclare(
                queue: GetTableQueueName(),
                durable: true,
                exclusive: false,
                autoDelete: false
            );
            
            channel.QueueBind(
                queue: GetTableQueueName(),
                exchange: options.ExchangeName,
                routingKey:"DbWorkers");
            
            
            // Use message type as the routing key so the message lands in that table's durable queue.
            channel.BasicPublish(
                exchange: options.ExchangeName,
                routingKey: "DbWorkers",
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

    private static IConnection CreateConnection(DbBrokerOptions options)
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
    
    private static string GetTableQueueName()
    {
        return "db.events";
    }
    
}
