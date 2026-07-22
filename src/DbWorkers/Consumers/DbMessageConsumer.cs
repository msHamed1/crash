using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Contracts.Messaging.DbWorkers;
using Crash.Domain.Options;
using DbWorkers.Application;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DbWorkers.Consumers;

public class DbMessageConsumer(
    DbBrokerOptions options,
    IServiceScopeFactory scopeFactory,

    ILogger<DbMessageConsumer> logger) : BackgroundService
    
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web)
        {
            Converters =
            {
                new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
            }
        };
    
    private readonly object _channelLock = new();

    protected  override async Task ExecuteAsync(CancellationToken stoppingToken)
    {

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
             await ConsumerAsync(stoppingToken);

            }

            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)

            {

                return;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                await Task.Delay(
                    TimeSpan.FromSeconds(5),
                    stoppingToken);
            
            }
        }
    }

    private async Task ConsumerAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.HostName,
            Port = options.Port,
            
            UserName = options.UserName,
            Password = options.Password,
            DispatchConsumersAsync = true,
            
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            RequestedHeartbeat = TimeSpan.FromSeconds(5),
        };
        
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        ConfigureTopology(channel);
        
        channel.BasicQos(
            prefetchSize: 0,
            prefetchCount: 10,
            global: false);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (model, ea) =>
        {
            DbWorkerMessageEnvelope? message = null;

            try
            {
                message = Deserialize(ea.Body);
                ValidateMessage(message);
                logger.LogInformation(
                    "Processing DB event {MessageId}, type {MessageType}, " +
                    "table {TableId}, round {RoundId}, sequence {Sequence}.",
                    message.MessageId,
                    message.Type,
                    message.Payload.TableId,
                    message.Payload.RoundId,
                    message.Payload.Sequence);

                await using var scope = scopeFactory.CreateAsyncScope();
                var processor = scope.ServiceProvider
                    .GetRequiredService<IDbWorkerMessageProcessor>();
                // This method must return only after the DB transaction commits

                var result = await processor.ProcessAsync(
                    message,
                    stoppingToken);

                PublishResult(channel, message, result);

                logger.LogInformation(
                    "DB event {MessageId} completed. AlreadyProcessed={AlreadyProcessed}.",
                    message.MessageId,
                    result.AlreadyProcessed);

                SafeAck(channel, ea.DeliveryTag);

            }
            catch (OperationCanceledException)when (stoppingToken.IsCancellationRequested)
            {
                // Do not ACK or NACK. Closing the channel causes RabbitMQ
                // to requeue this unacknowledged message.
            }catch (JsonException exception)
            {
                logger.LogError(
                    exception,
                    "Invalid DB event JSON. Sending message to dead-letter queue.");

                SafeReject(channel, ea.DeliveryTag);
            }
            catch (InvalidDbMessageException exception)
            {
                logger.LogError(
                    exception,
                    "Invalid DB event {MessageId}. Sending to dead-letter queue.",
                    message?.MessageId);

                SafeReject(channel, ea.DeliveryTag);
            }
            catch (PermanentDbMessageException exception)
            {
                logger.LogCritical(
                    exception,
                    "DB event {MessageId} requires investigation.",
                    message?.MessageId);

                SafeReject(channel, ea.DeliveryTag);
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "Temporary failure processing DB event {MessageId}. " +
                    "Message will be retried.",
                    message?.MessageId);

                SafeNack(
                    channel,
                    ea.DeliveryTag,
                    requeue: true);
            }

        };
        
        channel.BasicConsume(
            queue: "db.events",
            autoAck: false,
            consumer: consumer);

        logger.LogInformation("DB worker started consuming db.events.");

        await Task.Delay(
            Timeout.InfiniteTimeSpan,
            stoppingToken);
    }

   // Configures all RabbitMQ exchanges, queues, and bindings used by this worker.
private void ConfigureTopology(IModel channel)
{
    // Creates the dead-letter exchange name.
    // Example: if ExchangeName is "casino.events",
    // this becomes "casino.events.dead-letter".
    var deadLetterExchange = $"{options.ExchangeName}.dead-letter";

    // Declares the main exchange used for normal messages.
    channel.ExchangeDeclare(
        // The exchange name comes from your application configuration.
        exchange: options.ExchangeName,

        // Direct means messages are routed using an exact routing-key match.
        type: ExchangeType.Direct,

        // RabbitMQ keeps this exchange after a broker restart.
        durable: true,

        // RabbitMQ will not automatically delete the exchange when unused.
        autoDelete: false);

    channel.ExchangeDeclare(
        exchange: options.ExchangeResultName,
        type: ExchangeType.Direct,
        durable: true,
        autoDelete: false);

    // Declares a separate exchange for failed/dead-lettered messages.
    channel.ExchangeDeclare(
        // Name of the dead-letter exchange created above.
        exchange: deadLetterExchange,

        // Uses exact routing-key matching.
        type: ExchangeType.Direct,

        // Keeps the exchange after RabbitMQ restarts.
        durable: true,

        // Does not automatically delete the exchange when unused.
        autoDelete: false);

    // Declares the investigation queue.
    // Failed messages will eventually be sent to this queue.
    channel.QueueDeclare(
        // The name of the queue.
        queue: "db.events.investigation",

        // Keeps the queue after RabbitMQ restarts.
        durable: true,

        // The queue is not limited to this connection.
        // Other consumers and connections can access it.
        exclusive: false,

        // RabbitMQ will not delete the queue when consumers disconnect.
        autoDelete: false);

    // Connects the investigation queue to the dead-letter exchange.
    channel.QueueBind(
        // The destination queue.
        queue: "db.events.investigation",

        // Messages come from the dead-letter exchange.
        exchange: deadLetterExchange,

        // Only messages with this exact routing key enter the queue,
        // because the exchange type is Direct.
        routingKey: "DbWorkers.Dead");

    // Declares the main queue used by the database worker.
    channel.QueueDeclare(
        // Name of the normal processing queue.
        queue: "db.events",

        // Keeps the queue after RabbitMQ restarts.
        durable: true,

        // The queue can be accessed by multiple connections.
        exclusive: false,

        // The queue remains even when no consumers are connected.
        autoDelete: false,

        // Additional RabbitMQ queue configuration.
        arguments: new Dictionary<string, object>
        {
            // When a message becomes a dead letter,
            // RabbitMQ publishes it to this exchange.
            ["x-dead-letter-exchange"] = deadLetterExchange,

            // RabbitMQ replaces the original routing key with this routing key
            // when publishing the failed message to the dead-letter exchange.
            ["x-dead-letter-routing-key"] = "DbWorkers.Dead"
        });

    // Connects the main queue to the main exchange.
    channel.QueueBind(
        // The destination queue.
        queue: "db.events",

        // The normal application exchange.
        exchange: options.ExchangeName,

        // Because this is a Direct exchange, only messages published with
        // the exact routing key "DbWorkers" will enter this queue.
        routingKey: "DbWorkers");
}

private static DbWorkerMessageEnvelope Deserialize(
    ReadOnlyMemory<byte> body)
{
    var json = Encoding.UTF8.GetString(body.Span);

    return JsonSerializer.Deserialize<DbWorkerMessageEnvelope>(
               json,
               JsonOptions)
           ?? throw new JsonException("DB worker message is empty.");
}

private static void ValidateMessage(DbWorkerMessageEnvelope message)
{
    var valid = message switch
    {
        {
            Type: DbWorkerMessageType.BetAccepted,
            Payload: BetAcceptedForPersistence
        } => true,

        {
            Type: DbWorkerMessageType.BetSettled,
            Payload: BetSettledForPersistence
        } => true,

        {
            Type: DbWorkerMessageType.BetCancelled,
            Payload: BetCanceledForPersistence
        } => true,

        _ => false
    };

    if (!valid)
    {
        throw new InvalidDbMessageException(
            $"Message type {message.Type} does not match payload " +
            $"{message.Payload.GetType().Name}.");
    }
}

private void PublishResult(
    IModel channel,
    DbWorkerMessageEnvelope message,
    DbMessageProcessResult processResult)
{
    var (playerId, type, settlementStatus, payout, profitLoss, multiplier, settledAt) =
        message.Payload switch
        {
            BetAcceptedForPersistence accepted =>
                (accepted.PlayerId, DbWorkerResultMessageType.BetAccepted,
                    (BetSettlementStatus?)null, 0m, 0m, (decimal?)null, (DateTimeOffset?)null),
            BetSettledForPersistence settled =>
                (settled.PlayerId, DbWorkerResultMessageType.BetSettled,
                    (BetSettlementStatus?)settled.Status, settled.PayoutAmount,
                    settled.ProfitLoss, settled.CashoutMultiplier,
                    (DateTimeOffset?)settled.SettledAt),
            BetCanceledForPersistence cancelled =>
                (cancelled.PlayerId, DbWorkerResultMessageType.BetCancelled,
                    (BetSettlementStatus?)null, 0m, 0m, (decimal?)null,
                    (DateTimeOffset?)message.CreatedAt),
            _ => throw new InvalidDbMessageException("Unsupported DB result payload.")
        };

    var betId = message.Payload switch
    {
        BetAcceptedForPersistence accepted => accepted.BetId,
        BetSettledForPersistence settled => settled.BetId,
        BetCanceledForPersistence cancelled => cancelled.BetId,
        _ => throw new InvalidDbMessageException("Unsupported DB result payload.")
    };

    var result = new BetPersistenceResult(
        MessageId: Guid.NewGuid(),
        CausationMessageId: message.MessageId,
        BetId: betId,
        PlayerId: playerId,
        TableId: message.Payload.TableId,
        RoundId: message.Payload.RoundId,
        Sequence: message.Payload.Sequence,
        Type: type,
        Status: processResult.AlreadyProcessed
            ? DbWorkerResultStatus.AlreadyProcessed
            : DbWorkerResultStatus.Committed,
        SettlementStatus: settlementStatus,
        UpdatedBalance: processResult.UpdatedBalance,
        PayoutAmount: payout,
        ProfitLoss: profitLoss,
        CashoutMultiplier: multiplier,
        SettledAt: settledAt,
        ErrorCode: null,
        ErrorMessage: null,
        CompletedAt: DateTimeOffset.UtcNow);

    var body = JsonSerializer.SerializeToUtf8Bytes(result, JsonOptions);
    var properties = channel.CreateBasicProperties();
    properties.Persistent = true;
    properties.ContentType = "application/json";
    properties.MessageId = result.MessageId.ToString();

    lock (_channelLock)
    {
        channel.BasicPublish(
            exchange: options.ExchangeResultName,
            routingKey: $"table.{message.Payload.TableId}",
            basicProperties: properties,
            body: body);
    }
}

private void SafeAck(IModel channel, ulong deliveryTag)
{
    lock (_channelLock)
    {
        if (channel.IsOpen)
            channel.BasicAck(deliveryTag, multiple: false);
    }
}


private void SafeNack(
    IModel channel,
    ulong deliveryTag,
    bool requeue)
{
    lock (_channelLock)
    {
        if (channel.IsOpen)
        {
            channel.BasicNack(
                deliveryTag,
                multiple: false,
                requeue: requeue);
        }
    }
}

private void SafeReject(IModel channel, ulong deliveryTag)
{
    lock (_channelLock)
    {
        if (channel.IsOpen)
        {
            // requeue:false sends it through the configured dead-letter exchange.
            channel.BasicReject(
                deliveryTag,
                requeue: false);
        }
    }
}
}
