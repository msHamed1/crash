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
    private static readonly TimeSpan ConfirmTimeout = TimeSpan.FromSeconds(5);
    
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    
    
    
    private readonly FanoutOptions options;
    private IConnection? connection;
    private IModel? transientChannel;
    private IModel? reliableChannel;
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
    private (IModel Transient, IModel Reliable) GetChannels()
    {
        if (connection?.IsOpen == true
            && transientChannel?.IsOpen == true
            && reliableChannel?.IsOpen == true)
        {
            return (transientChannel, reliableChannel);
        }

        transientChannel?.Dispose();
        reliableChannel?.Dispose();
        connection?.Dispose();

        connection = CreateConnection(options);
        transientChannel = connection.CreateModel();
        reliableChannel = connection.CreateModel();

        ConfigureTopology(transientChannel);
        ConfigureTopology(reliableChannel);
        reliableChannel.ConfirmSelect();

        return (transientChannel, reliableChannel);
    }

    private void ConfigureTopology(IModel rabbitChannel)
    {
        rabbitChannel.ExchangeDeclare(
            exchange: options.ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false);
        rabbitChannel.QueueDeclare(
            queue: options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false);
        rabbitChannel.QueueBind(
            queue: options.QueueName,
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey);
    }

    public Task PublishAsync(ToClient message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var json = JsonSerializer.Serialize(message, message.GetType(), JsonOptions);
        var body = Encoding.UTF8.GetBytes(json);
        lock (publishLock)
        {
            var isTransientTick = message is RoundTick;
            var maxAttempts = isTransientTick ? 1 : 3;

            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    Publish(body, message, isTransientTick);
                    break;
                }
                catch when (attempt < maxAttempts)
                {
                    // A confirm can be lost after the broker accepted the message. Retrying is
                    // intentionally at-least-once; MessageId/round sequence let consumers deduplicate.
                    ResetConnection();
                    Thread.Sleep(TimeSpan.FromMilliseconds(100 * attempt));
                }
            }
        }
        
        return Task.CompletedTask;

    }

    private void Publish(byte[] body, ToClient message, bool isTransientTick)
    {
        var channels = GetChannels();
        var rabbitChannel = isTransientTick ? channels.Transient : channels.Reliable;
        var properties = rabbitChannel.CreateBasicProperties();
        // Ticks are replaceable snapshots. Lifecycle messages must survive broker restart.
        properties.Persistent = !isTransientTick;
        properties.ContentType = "application/json";
        properties.MessageId = message.MessageId;
        properties.Type = message.MessageType;
        properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        rabbitChannel.BasicPublish(
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey,
            mandatory: !isTransientTick,
            basicProperties: properties,
            body: body);

        if (!isTransientTick)
        {
            // New-round/crash publication is only complete after the broker confirms it.
            rabbitChannel.WaitForConfirmsOrDie(ConfirmTimeout);
        }
    }

    private void ResetConnection()
    {
        transientChannel?.Dispose();
        reliableChannel?.Dispose();
        connection?.Dispose();
        transientChannel = null;
        reliableChannel = null;
        connection = null;
    }

    public void Dispose()
    {
        ResetConnection();
    }
}
