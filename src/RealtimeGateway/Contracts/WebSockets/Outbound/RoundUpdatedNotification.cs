namespace RealtimeGateway.Contracts.WebSockets.Outbound;

public sealed record RoundUpdatedNotification(
    string MessageType,
    string MessageId,
    long TableId,
    long RoundId,
    decimal CurrentMultiplier,
    long TickSequence);
