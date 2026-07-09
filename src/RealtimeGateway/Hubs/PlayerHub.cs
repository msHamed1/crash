using Crash.Domain.Contracts;
using Crash.Domain.Contracts.BetMessages;
using Crash.Domain.Contracts.PlayerMessages;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
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
        
        var now = DateTime.UtcNow;
        var correlationId =  Guid.NewGuid().ToString();
        var envelope = new PlayerJoinedEvent
        {
            CorrelationId = correlationId,
            CreatedAtUtc = now,
            TableId = GetTableGroup(playerContext.TableId),
            ProcessedAtGatewayUtc = now,
            ProcessedAtClientUtc = now,
            Data = new PlayerJoinedData
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

    public async Task Bet(PlaceBetReqData request)
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
        var envelope = new PlaceBetReqEvent
        {
            CorrelationId = correlationId,
            CreatedAtUtc = now,
            TableId = GetTableGroup(playerContext.TableId),
            ProcessedAtGatewayUtc = now,
            ProcessedAtClientUtc = now,
            Data = new PlaceBetReqData
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
            MessageId = request.CorrelationId,
            Type =  envelope.MessageType,
        }, Context.ConnectionAborted);

    }





}
