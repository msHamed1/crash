using System.Text.Json.Serialization;

namespace Crash.Contracts.Messaging.DbWorkers;

/// <summary>
/// Identifies the database operation represented by a DB-worker message.
/// The values describe business events instead of generic CRUD operations.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum DbWorkerMessageType
{
    BetAccepted,
    BetSettled
}

/// <summary>
/// A financially accepted bet may only finish in one of these terminal states.
/// Cancellation is intentionally not a settlement option.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BetSettlementStatus
{
    CashedOut,
    Lost,
    Won,
    Cashback
}

/// <summary>
/// Shared ordering and ownership metadata used by every DB-worker payload.
/// The polymorphic discriminator allows the consumer to deserialize the
/// envelope without coupling the contract to domain entities.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$payloadType")]
[JsonDerivedType(typeof(BetAcceptedForPersistence), "bet-accepted")]
[JsonDerivedType(typeof(BetSettledForPersistence), "bet-settled")]
public abstract record DbWorkerMessagePayload(
    long TableId,
    long RoundId,
    long FencingToken,
    long Sequence);

public sealed record BetAcceptedForPersistence(
    string BetId,
    long PlayerId,
    long TableId,
    long RoundId,
    decimal StakeAmount,
    string Currency,
    decimal? AutoCashoutMultiplier,
    long FencingToken,
    long Sequence,
    DateTimeOffset AcceptedAt)
    : DbWorkerMessagePayload(TableId, RoundId, FencingToken, Sequence);

public sealed record BetSettledForPersistence(
    string BetId,
    long PlayerId,
    long TableId,
    long RoundId,
    BetSettlementStatus Status,
    decimal PayoutAmount,
    decimal ProfitLoss,
    decimal? CashoutMultiplier,
    long FencingToken,
    long Sequence,
    DateTimeOffset SettledAt)
    : DbWorkerMessagePayload(TableId, RoundId, FencingToken, Sequence);

/// <summary>
/// MessageId is the idempotency key recorded by the DB worker in the same
/// transaction as the business write.
/// </summary>
public sealed record DbWorkerMessageEnvelope(
    Guid MessageId,
    DbWorkerMessageType Type,
    DbWorkerMessagePayload Payload,
    DateTimeOffset CreatedAt);
