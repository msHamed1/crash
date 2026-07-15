using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Contracts.Messaging.EngineToGateway;
using Crash.Contracts.Messaging.EngineToGateway.Rounds;
using Crash.Domain.Options;
using RabbitMQ.Client;

namespace GameEngine.Messaging;



public interface IClientMessagePublisher
{
    Task PublishAsync(GatewayMessage message, CancellationToken ct);
}
public class ClientMessagePublisher:IClientMessagePublisher,IDisposable
{
    private static readonly TimeSpan ConfirmTimeout = TimeSpan.FromSeconds(5);
    
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    
    
    
    private readonly FanoutOptions options;
    private IConnection? transientConnection;
    private IConnection? reliableConnection;
    private IModel? transientChannel;
    private IModel? reliableChannel;
    private readonly object transientPublishLock = new();
    private readonly SemaphoreSlim reliablePublishGate = new(1, 1);
    private TaskCompletionSource<bool>? pendingReliableConfirm;

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
    private IModel GetTransientChannel()
    {
        if (transientConnection?.IsOpen == true && transientChannel?.IsOpen == true)
            return transientChannel;

        transientChannel?.Dispose();
        transientConnection?.Dispose();
        transientConnection = CreateConnection(options);
        transientChannel = transientConnection.CreateModel();
        ConfigureTopology(transientChannel);
        return transientChannel;
    }

    private IModel GetReliableChannel()
    {
        if (reliableConnection?.IsOpen == true && reliableChannel?.IsOpen == true)
            return reliableChannel;

        reliableChannel?.Dispose();
        reliableConnection?.Dispose();
        reliableConnection = CreateConnection(options);
        reliableChannel = reliableConnection.CreateModel();
        ConfigureTopology(reliableChannel);
        reliableChannel.ConfirmSelect();
        var confirmingChannel = reliableChannel;
        reliableChannel.BasicAcks += (_, _) =>
        {
            if (ReferenceEquals(reliableChannel, confirmingChannel))
                pendingReliableConfirm?.TrySetResult(true);
        };
        reliableChannel.BasicNacks += (_, _) =>
        {
            if (ReferenceEquals(reliableChannel, confirmingChannel))
                pendingReliableConfirm?.TrySetResult(false);
        };
        return reliableChannel;
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

    public async Task PublishAsync(GatewayMessage message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var json = JsonSerializer.Serialize(message, message.GetType(), JsonOptions);
        var body = Encoding.UTF8.GetBytes(json);
        var isTransientTick = message is RoundUpdated;

        if (isTransientTick)
        {
            lock (transientPublishLock)
            {
                try
                {
                    PublishTransient(body, message);
                }
                catch
                {
                    ResetTransientConnection();
                    throw;
                }
            }
            return;
        }

        await reliablePublishGate.WaitAsync(ct);
        try
        {
            for (var attempt = 1; attempt <= 3; attempt++)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    var rabbitChannel = GetReliableChannel();
                    var confirm = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    pendingReliableConfirm = confirm;
                    PublishReliable(rabbitChannel, body, message);

                    var wasAcknowledged = await confirm.Task.WaitAsync(ConfirmTimeout, ct);
                    if (!wasAcknowledged)
                        throw new IOException($"RabbitMQ rejected {message.MessageType} message {message.MessageId}.");

                    return;
                }
                catch when (attempt < 3)
                {
                    // A confirm can be lost after the broker accepted the message. Retrying is
                    // intentionally at-least-once; MessageId/round sequence let consumers deduplicate.
                    ResetReliableConnection();
                    await Task.Delay(TimeSpan.FromMilliseconds(100 * attempt), ct);
                }
                catch
                {
                    ResetReliableConnection();
                    throw;
                }
            }
        }
        finally
        {
            pendingReliableConfirm = null;
            reliablePublishGate.Release();
        }

    }

    private void PublishTransient(byte[] body, GatewayMessage message)
    {
        var rabbitChannel = GetTransientChannel();
        var properties = rabbitChannel.CreateBasicProperties();
        properties.Persistent = false;
        properties.ContentType = "application/json";
        properties.MessageId = message.MessageId;
        properties.Type = message.MessageType;
        properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        rabbitChannel.BasicPublish(
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey,
            mandatory: false,
            basicProperties: properties,
            body: body);
    }

    private void PublishReliable(IModel rabbitChannel, byte[] body, GatewayMessage message)
    {
        var properties = rabbitChannel.CreateBasicProperties();
        properties.Persistent = true;
        properties.ContentType = "application/json";
        properties.MessageId = message.MessageId;
        properties.Type = message.MessageType;
        properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        rabbitChannel.BasicPublish(
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey,
            mandatory: true,
            basicProperties: properties,
            body: body);
    }

    private void ResetTransientConnection()
    {
        transientChannel?.Dispose();
        transientConnection?.Dispose();
        transientChannel = null;
        transientConnection = null;
    }

    private void ResetReliableConnection()
    {
        reliableChannel?.Dispose();
        reliableConnection?.Dispose();
        reliableChannel = null;
        reliableConnection = null;
    }

    public void Dispose()
    {
        lock (transientPublishLock)
        {
            ResetTransientConnection();
        }
        ResetReliableConnection();
        reliablePublishGate.Dispose();
    }
}
