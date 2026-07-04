using Crash.Domain.Entities;

namespace Crash.Domain.Contracts;

public enum DbWorkerMessageType
{
    Create,
    Update,
    Delete,
}

public sealed record DbWorkerMessagePayload(
    string RoundId,
    Round Round,
    Bet Bet,
    string TableId,
    string PlayerId
);

public sealed record DbWorkerMessageEnvelope(
    Guid MessageId,
    DbWorkerMessageType Type,
    DbWorkerMessagePayload Payload,
    DateTimeOffset ReceivedAt
     
    );