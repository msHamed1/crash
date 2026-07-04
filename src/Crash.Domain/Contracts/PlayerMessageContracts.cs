namespace Crash.Domain.Contracts;

public enum PlayerMessageType
{
    Bet = 1
}

public sealed record PlayerClientMessage(
    PlayerMessageType Type,
    decimal? Amount,
    string? Currency,
    decimal? AutoCashOut);

public sealed record PlayerMessageEnvelope(
    Guid MessageId,
    string TableId,
    string PlayerId,
    PlayerMessageType Type,
    PlayerClientMessage Payload,
    DateTimeOffset ReceivedAt);
