namespace RealtimeGateway.Contracts.WebSockets.Outbound;

public sealed record RoundCreatedNotification(
    string MessageType,
    string MessageId,
    long TableId,
    long RoundId,
    decimal CurrentMultiplier,
    DateTimeOffset StartsAt,
    bool IsCrashed);
