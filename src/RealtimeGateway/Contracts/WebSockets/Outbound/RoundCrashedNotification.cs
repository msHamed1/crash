namespace RealtimeGateway.Contracts.WebSockets.Outbound;

public sealed record RoundCrashedNotification(
    string MessageType,
    string MessageId,
    long TableId,
    long RoundId,
    decimal CurrentMultiplier,
    bool IsCrashed,
    long TickSequence);
