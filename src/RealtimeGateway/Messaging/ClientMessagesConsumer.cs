using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Domain.Contracts;
using Crash.Domain.Contracts.ServerMessages;
using Crash.Domain.Options;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RealtimeGateway.Hubs;

namespace RealtimeGateway.Messaging;

public class ClientMessagesConsumer(
    FanoutOptions options,
    IHubContext<PlayerHub> hubContext,

    ILogger<ClientMessagesConsumer> logger) :BackgroundService
{
    
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };
    
    private readonly object _channelLock = new();
    protected async override Task ExecuteAsync(CancellationToken stoppingToken)
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
                logger.LogWarning(e, "RealTime message consumer failed. Retrying in 5s.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            
        }

        return;
    }
    
   

    private void Consume(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.HostName,
            UserName = options.UserName,
            Password = options.Password,
            Port = options.Port,
            DispatchConsumersAsync = true
        };
        
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            exchange: options.ExchangeName,
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
                await DispatchToClientAsync(json, ct);
               
                
                SafeAck(channel, args.DeliveryTag);

            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to process player message.");
                SafeNack(channel, args.DeliveryTag, requeue: true);            }
            await Task.CompletedTask;

        };

        channel.QueueDeclare(
            queue: options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false);
        
        channel.QueueBind(
            queue: options.QueueName,
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey);

        channel.BasicConsume(
            queue: options.QueueName,
            autoAck: false,
            consumer: consumer);
        
        logger.LogInformation(
            "DbWorker is consuming DbMessages messages for tables: DbWorkers."
        );
        WaitHandle.WaitAny([ct.WaitHandle]);


    }
    
    private async Task DispatchToClientAsync(string json, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(json);

        var messageType = doc.RootElement
            .GetProperty("messageType")
            .GetString();

        switch (messageType)
        {
            case "NewRoundInfo":
            {
                var message = JsonSerializer.Deserialize<NewRoundInfo>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid NewRoundInfo message.");

                await hubContext.Clients
                    .Group(message.TableId.ToString())
                    .SendAsync(message.MessageType, message, ct);

                break;
            }

            case "RoundTick":
            {
                var message = JsonSerializer.Deserialize<RoundTick>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid RoundTick message.");

                await hubContext.Clients
                    .Group(message.TableId.ToString())
                    .SendAsync(message.MessageType, message, ct);

                break;
            }
            
            case "RoundCrashed":
            {
                var message = JsonSerializer.Deserialize<RoundCrashed>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid RoundTick message.");

                await hubContext.Clients
                    .Group(message.TableId.ToString())
                    .SendAsync(message.MessageType, message, ct);

                break;
            }

            case "CurrentState":
            {
                var message = JsonSerializer.Deserialize<CurrentState>(json, JsonOptions)
                              ?? throw new InvalidOperationException("Invalid CurrentState message.");
                
                await hubContext.Clients.Group(message.TableId.ToString())
                    .SendAsync(message.MessageType, message, ct);
                // if (!string.IsNullOrWhiteSpace(message.ConnectionId))
                // {
                //     await hubContext.Clients
                //         .Client(message.ConnectionId)
                //         .SendAsync(message.MessageType, message, ct);
                // }
                // else
                // {
                //     await hubContext.Clients
                //         .Group($"player:{message.PlayerId}")
                //         .SendAsync(message.MessageType, message, ct);
                // }

                break;
            }

            default:
                throw new InvalidOperationException($"Unsupported client message type: {messageType}");
        }
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
}