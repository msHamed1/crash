using Microsoft.AspNetCore.SignalR;
using RealtimeGateway.Messaging;

namespace RealtimeGateway.Hubs;

public sealed class PlayerHub(
    IJwtConnectionValidator jwtConnectionValidator,
    IPlayerMessagePublisher publisher) : Hub
{
    private const string ConnectionContextKey = "player-context";

    public override Task OnConnectedAsync()
    {
        // The connection identity is fixed at connect time and comes from the signed JWT, not from client messages.
        Context.Items[ConnectionContextKey] = jwtConnectionValidator.Validate(Context);
        return base.OnConnectedAsync();
    }

    public async Task SendMessage(PlayerClientMessage message)
    {
        if (message.Type != PlayerMessageType.Bet)
        {
            throw new HubException("Only Bet messages are supported.");
        }

        var playerContext = GetPlayerContext();
        var envelope = new PlayerMessageEnvelope(
            Guid.NewGuid(),
            playerContext.TableId,
            playerContext.PlayerId,
            message.Type,
            message,
            DateTimeOffset.UtcNow);

        await publisher.PublishAsync(envelope, Context.ConnectionAborted);
    }

    private PlayerConnectionContext GetPlayerContext()
    {
        if (Context.Items.TryGetValue(ConnectionContextKey, out var value)
            && value is PlayerConnectionContext playerContext)
        {
            return playerContext;
        }

        throw new HubException("Connection is not authenticated.");
    }
}
