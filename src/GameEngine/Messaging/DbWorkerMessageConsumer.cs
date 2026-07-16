using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Contracts.Messaging.DbWorkers;
using Crash.Domain.Options;
using GameEngine.Application.Commands.Bets;
using GameEngine.Services;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
namespace GameEngine.Messaging;

public class DbWorkerMessageConsumer(
    DbBrokerOptions options,
    GameEngineOptions gameEngineOptions,
    ILogger<DbWorkerMessageConsumer> logger,
  RoundsService  roundsService

    ): BackgroundService
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

    foreach (var (tableId, table) in gameEngineOptions.Tables)
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
            exchange: options.ExchangeName,

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
    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.Received += async (model, ea) =>
    {
        BetPersistenceResult? message = null;

        try
        {
            message = Deserialize(ea.Body);
            
            logger.LogInformation(
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
                Status = message.Status,
                ErrorCode =  message.ErrorCode,
                IsCreated = message is { Type: DbWorkerResultMessageType.Bet, Status: DbWorkerResultStatus.Committed },
                
            };
            
            await roundsService.EnqueueAsync(command, ct);
            
         //   SafeAck();

        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    };
}

    
    protected  override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {

            try
            {
//             await ConsumerAsync(stoppingToken);

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