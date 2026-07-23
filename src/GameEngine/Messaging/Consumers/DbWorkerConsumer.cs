using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Contracts.Messaging.DbWorkers;
using Crash.Domain.Options;
using GameEngine.Application.Commands.Bets;
using GameEngine.Application.Results;
using GameEngine.Services;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace GameEngine.Messaging.Consumers;

public class DbWorkerConsumer(
    DbBrokerOptions options,
    GameEngineOptions gameEngineOptions,
    ILogger<DbWorkerConsumer> logger,
    BettingService  bettingService

    ): BackgroundService
{
    private const ushort ResultPrefetchCount = 25;
    private const int MaxRetryAttempts = 5;

    private readonly ConcurrentDictionary<Guid, int> _retryAttempts = new();
    
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web)
        {
            Converters =
            {
                new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
            }
        };
    
    private readonly object _channelLock = new();

    private void SafeAck(IModel channel, ulong deliveryTag)
    {
        lock (_channelLock)
        {
            if (channel.IsOpen)
                channel.BasicAck(deliveryTag, multiple: false);
        }
    }

    private async Task RetryOrAcknowledgeAsync(
        IModel channel,
        ulong deliveryTag,
        Guid messageId,
        string reason,
        CancellationToken ct)
    {
        var attempt = _retryAttempts.AddOrUpdate(
            messageId,
            1,
            static (_, current) => current + 1);

        if (attempt > MaxRetryAttempts)
        {
            _retryAttempts.TryRemove(messageId, out _);
            logger.LogCritical(
                "Acknowledging DB result {MessageId} after {RetryCount} retries. " +
                "The committed DB state remains authoritative. Reason: {Reason}",
                messageId,
                MaxRetryAttempts,
                reason);
            SafeAck(channel, deliveryTag);
            return;
        }

        var delay = TimeSpan.FromMilliseconds(
            Math.Min(2_000, 100 * Math.Pow(2, attempt - 1)));

        logger.LogWarning(
            "Retrying DB result {MessageId} in {DelayMs} ms. Attempt {Attempt}/{MaxAttempts}. Reason: {Reason}",
            messageId,
            delay.TotalMilliseconds,
            attempt,
            MaxRetryAttempts,
            reason);

        await Task.Delay(delay, ct);
        SafeNack(channel, deliveryTag, requeue: true);
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
    
       // Configures all RabbitMQ exchanges, queues, and bindings used by this worker.
private void ConfigureTopology(IModel channel)
{
    // Creates the dead-letter exchange name.
    // Example: if ExchangeName is "casino.events",
 
    // Declares the main exchange used for normal messages.
    channel.ExchangeDeclare(
        // The exchange name comes from your application configuration.
        exchange: options.ExchangeResultName,

        // Direct means messages are routed using an exact routing-key match.
        type: ExchangeType.Direct,

        // RabbitMQ keeps this exchange after a broker restart.
        durable: true,

        // RabbitMQ will not automatically delete the exchange when unused.
        autoDelete: false);

}

private void ConfigureTableResultQueue(IModel channel, long tableId)
{
    lock (_channelLock)
    {
        // Declares the main queue used by the database worker.
        channel.QueueDeclare(
            // Name of the normal processing queue.
            queue: $"table.{tableId}.db-results",

            // Keeps the queue after RabbitMQ restarts.
            durable: true,

            // The queue can be accessed by multiple connections.
            exclusive: false,

            // The queue remains even when no consumers are connected.
            autoDelete: false );
        
        // Connects the main queue to the main exchange.
        channel.QueueBind(
            // The destination queue.
            queue: $"table.{tableId}.db-results",

            // The normal application exchange.
            exchange: options.ExchangeResultName,

            // Because this is a Direct exchange, only messages published with
            // the exact routing key "DbWorkers" will enter this queue.
            routingKey:$"table.{tableId}");
    }
}

private static BetPersistenceResult Deserialize(
    ReadOnlyMemory<byte> body)
{
    var json = Encoding.UTF8.GetString(body.Span);

    return JsonSerializer.Deserialize<BetPersistenceResult>(
               json,
               JsonOptions)
           ?? throw new JsonException("DB worker message is empty.");
}

private async Task ConsumerAsync(CancellationToken ct)
{
    var factory = new ConnectionFactory
    {
        HostName = options.HostName,
        UserName = options.UserName,
        Password = options.Password,
        Port = options.Port,
        DispatchConsumersAsync = true,
    };
    
    using var connection = factory.CreateConnection();
    using var channel = connection.CreateModel();
    
    ConfigureTopology(channel);
    channel.BasicQos(
        prefetchSize: 0,
        prefetchCount: ResultPrefetchCount,
        global: false);

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.Received += async (model, ea) =>
    {
        BetPersistenceResult? message = null;

        try
        {
            message = Deserialize(ea.Body);
            
            logger.LogDebug(
                "Processing DB Response {MessageId}, type {Type}, " +
                "Status {Status}, PlayerId {PlayerId}, RoundId {RoundId}.",
                message.MessageId,
                message.Type,
                message.Status,
                message.PlayerId,
                message.RoundId);

            var command = new DbBetPersistenceCompletedCommand
            {
                CausationMessageId = message.CausationMessageId,
                BetId = message.BetId,
                TableId=message.TableId.ToString(),
                RoundId = message.RoundId.ToString(),
                PlayerId= message.PlayerId,

                Status = message.Status,
                ResultType = message.Type,
                SettlementStatus = message.SettlementStatus,
                UpdatedBalance = message.UpdatedBalance,
                PayoutAmount = message.PayoutAmount,
                ProfitLoss = message.ProfitLoss,
                CashoutMultiplier = message.CashoutMultiplier,
                SettledAt = message.SettledAt,
                ErrorCode =  message.ErrorCode,
                ErrorMessage = message.ErrorMessage,
            };
            
             
            
            var outcome = await bettingService.ProcessBetPersistenceCompleted(command, ct);

            switch (outcome.Disposition)
            {
                case DbResultHandlingDisposition.Retryable:
                    await RetryOrAcknowledgeAsync(
                        channel,
                        ea.DeliveryTag,
                        message.MessageId,
                        outcome.Reason,
                        ct);
                    break;

                case DbResultHandlingDisposition.Invalid:
                    _retryAttempts.TryRemove(message.MessageId, out _);
                    logger.LogError(
                        "Acknowledging invalid DB result {MessageId}. Reason: {Reason}",
                        message.MessageId,
                        outcome.Reason);
                    SafeAck(channel, ea.DeliveryTag);
                    break;

                case DbResultHandlingDisposition.Stale:
                    _retryAttempts.TryRemove(message.MessageId, out _);
                    logger.LogDebug(
                        "Acknowledging stale DB result {MessageId}. Reason: {Reason}",
                        message.MessageId,
                        outcome.Reason);
                    SafeAck(channel, ea.DeliveryTag);
                    break;

                default:
                    _retryAttempts.TryRemove(message.MessageId, out _);
                    SafeAck(channel, ea.DeliveryTag);
                    break;
            }

        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Closing the channel returns this unacknowledged result to RabbitMQ.
        }
        catch (JsonException exception)
        {
            logger.LogError(
                exception,
                "Acknowledging malformed DB result because redelivery cannot repair its JSON.");
            SafeAck(channel, ea.DeliveryTag);
        }
        catch (Exception exception)
        {
            if (message is null)
            {
                logger.LogError(
                    exception,
                    "Acknowledging unreadable DB result because it has no usable message identity.");
                SafeAck(channel, ea.DeliveryTag);
                return;
            }

            logger.LogError(
                exception,
                "Unexpected failure while processing DB result {MessageId}.",
                message.MessageId);

            await RetryOrAcknowledgeAsync(
                channel,
                ea.DeliveryTag,
                message.MessageId,
                exception.Message,
                ct);
        }
    };

    var consumedTables = new HashSet<long>();
    while (!ct.IsCancellationRequested)
    {
        foreach (var tableId in gameEngineOptions.Tables.Keys)
        {
            if (!consumedTables.Add(tableId))
                continue;

            ConfigureTableResultQueue(channel, tableId);
            lock (_channelLock)
            {
                channel.BasicConsume(
                    queue: $"table.{tableId}.db-results",
                    autoAck: false,
                    consumer: consumer);
            }
        }

        await Task.Delay(TimeSpan.FromSeconds(1), ct);
    }
}

    
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
}
