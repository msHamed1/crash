using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Domain.Contracts;
using Crash.Domain.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DbWorkers.Services;

public sealed class DbMessageConsumer (
    DbBrokerOptions brokerOptions,
    ILogger<DbMessageConsumer> logger) :BackgroundService
{
    
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    
    private readonly object _channelLock = new();
     protected override async Task ExecuteAsync(CancellationToken stoppingToken)
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
                logger.LogWarning(e, "DbWorker message consumer failed. Retrying in 5s.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

            }
             
        }

        return;
    }

    private void Consume(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = brokerOptions.HostName,
            UserName = brokerOptions.UserName,
            Password = brokerOptions.Password,
            Port = brokerOptions.Port,
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

        consumer.Received +=async (_, args) =>
        {
            try
            {
                var json = Encoding.UTF8.GetString(args.Body.ToArray());
                var message = JsonSerializer.Deserialize<DbWorkerMessageEnvelope>(json, JsonOptions) ?? throw new InvalidOperationException("Db Worker message body is empty.");
                logger.LogDebug(
                    "GameEngine {Type} received {RoundId} for table {TableId}, player {MessageId}",
                     message.Type,
                    message.Payload.RoundId,
                    message.Payload.TableId,
                    message.MessageId);
                
                SafeAck(channel, args.DeliveryTag);

            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to process player message.");
                SafeNack(channel, args.DeliveryTag, requeue: true);            }
            await Task.CompletedTask;

        };
        
        var queueName = GetTableQueueName();

        channel.QueueDeclare(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false);

         channel.QueueBind(
            queue: queueName,
            exchange: brokerOptions.ExchangeName,
            routingKey: "DbWorkers");

        channel.BasicConsume(
            queue: queueName,
            autoAck: false,
            consumer: consumer);
        
        logger.LogInformation(
            "DbWorker is consuming DbMessages messages for tables: DbWorkers."
            );
        
        WaitHandle.WaitAny([ct.WaitHandle]);


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
    
    private static string GetTableQueueName()
    {
        return "db.events";
    }
}