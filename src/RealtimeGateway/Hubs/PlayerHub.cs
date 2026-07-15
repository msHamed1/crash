using Crash.Contracts.Messaging.GatewayToEngine.Bets;
using Crash.Contracts.Messaging.GatewayToEngine.Players;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RealtimeGateway.Contracts.WebSockets.Inbound;
using RealtimeGateway.Jwt;
using RealtimeGateway.Messaging;

namespace RealtimeGateway.Hubs;

public sealed class PlayerHub(
    IJwtConnectionValidator jwtConnectionValidator,
    IPlayerMessagePublisher publisher,
    ILogger<PlayerHub> _logger) : Hub
{
    private const string ConnectionContextKey = "player-context";

    public override async Task OnConnectedAsync()
    {
        // The connection identity is fixed at connect time and comes from the signed JWT, not from client messages.
        var playerContext = jwtConnectionValidator.Validate(Context);
        Context.Items[ConnectionContextKey] = playerContext;
        
        _logger.LogInformation(
            "Player connected. PlayerId: {PlayerId}, TableId: {TableId}, ConnectionId: {ConnectionId}",
            playerContext.PlayerId,
            playerContext.TableId,
            Context.ConnectionId);      
        
        
        // Add Player to the Group (Table Group)
        await Groups.AddToGroupAsync(Context.ConnectionId, playerContext.TableId);
        // Private balance and bet responses are delivered only to this player's connections.
        await Groups.AddToGroupAsync(Context.ConnectionId, $"player:{playerContext.PlayerId}");
        
        var now = DateTime.UtcNow;
        var correlationId =  Guid.NewGuid().ToString();
        var envelope = new PlayerJoined
        {
            CorrelationId = correlationId,
            CreatedAtUtc = now,
            TableId = GetTableGroup(playerContext.TableId),
            ProcessedAtGatewayUtc = now,
            ProcessedAtClientUtc = now,
            Data = new PlayerJoinedPayload
            {
               
                PlayerId = playerContext.PlayerId,
                PlayerCode = playerContext.ExternalId
                 
            }
        };
        
        await publisher.PublishAsync(envelope,new PublisherOptions
        {
            TableId = playerContext.TableId,
            Timestamp =  new AmqpTimestamp( DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            MessageId = envelope.CorrelationId,
            Type =  envelope.MessageType,
        }, Context.ConnectionAborted);
        
        await base.OnConnectedAsync();
    }
    
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        var playerContext = GetPlayerContextOrNull();

        if (playerContext is not null)
        {
            await Groups.RemoveFromGroupAsync(
                Context.ConnectionId,
                playerContext.TableId);
            await Groups.RemoveFromGroupAsync(
                Context.ConnectionId,
                $"player:{playerContext.PlayerId}");

            _logger.LogInformation(
                "Player disconnected. PlayerId: {PlayerId}, TableId: {TableId}, ConnectionId: {ConnectionId}",
                playerContext.PlayerId,
                playerContext.TableId,
                Context.ConnectionId);
        }

        await base.OnDisconnectedAsync(exception);
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
    private static long GetTableGroup(string tableId)
    {
        return int.Parse(tableId);
    }
    
    private PlayerConnectionContext? GetPlayerContextOrNull()
    {
        if (Context.Items.TryGetValue(ConnectionContextKey, out var value)
            && value is PlayerConnectionContext playerContext)
        {
            return playerContext;

        }

        return null;
    }

    [HubMethodName("Bet")]
    public async Task PlaceBet(PlaceBetRequest request)
    {
        var playerContext = GetPlayerContext();
        if (request.Amount <= 0)
        {
            throw new HubException("Bet amount must be greater than zero.");
        }
        
        if (string.IsNullOrWhiteSpace(request.RoundId))
        {
            throw new HubException("RoundId is required.");
        }

        if (string.IsNullOrWhiteSpace(request.Currency))
        {
            throw new HubException("Currency is required.");
        }

        if (request.AutoCashoutEnabled == true &&
            request.AutoCashoutAt is null or <= 1)
        {
            throw new HubException("Auto cashout multiplier must be greater than 1.");
        }
        
        var now = DateTime.UtcNow;
        var correlationId = string.IsNullOrWhiteSpace(request.CorrelationId)
            ? Guid.NewGuid().ToString()
            : request.CorrelationId;
        var envelope = new PlaceBetRequested
        {
            CorrelationId = correlationId,
            CreatedAtUtc = now,
            TableId = GetTableGroup(playerContext.TableId),
            ProcessedAtGatewayUtc = now,
            ProcessedAtClientUtc = now,
            Data = new PlaceBetRequestPayload
            {
                RoundId = request.RoundId,
                PlayerId = playerContext.PlayerId,
                Amount = request.Amount,
                Currency = request.Currency,
                AutoCashoutAt = request.AutoCashoutAt,
                AutoCashoutEnabled = request.AutoCashoutEnabled
            }
        };
        
        await publisher.PublishAsync(envelope,new PublisherOptions
        {
            TableId = playerContext.TableId,
            Timestamp =  new AmqpTimestamp( DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            MessageId = correlationId,
            Type =  envelope.MessageType,
        }, Context.ConnectionAborted);

    }
    
    





}
