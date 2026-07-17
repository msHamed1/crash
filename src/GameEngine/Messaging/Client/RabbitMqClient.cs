using System.Text;
using System.Text.Json;

namespace GameEngine.Messaging.Client;
using RabbitMQ.Client;



public interface IRabbitMqClient
{
    Task PublishAsync<T>(
        string exchange,
        string routingKey,
        T message,
        CancellationToken ct);

    Task<IChannel> CreateChannelAsync(CancellationToken ct);
}
public sealed class RabbitMqClient(IConnection connection, JsonSerializerOptions jsonOptions)
    : IRabbitMqClient, IAsyncDisposable
{
    
    public Task<IChannel> CreateChannelAsync(CancellationToken ct)
    {
        connection.CreateChannelAsync(ct);
    }



    public async Task PublishAsync<T>(
        string exchange,
        string routingKey,
        T message,
        CancellationToken ct)
    {
        await using var channel =
            await connection.CreateChannelAsync(cancellationToken: ct);

        var json = JsonSerializer.Serialize(message, jsonOptions);
        var body = Encoding.UTF8.GetBytes(json);

        await channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: false,
            body: body,
            cancellationToken: ct);
    }

 
    public async ValueTask DisposeAsync()
    {
        await connection.DisposeAsync();

    }
}