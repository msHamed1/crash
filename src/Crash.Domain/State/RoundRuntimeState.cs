namespace Crash.Domain.State;


/// <summary>
/// Represents the runtime state of a Crash game round.
///
/// The object is continuously updated by the RoundTicker until the
/// round reaches its crash point.
///
/// Once the round finishes, it can be persisted and removed from memory.
/// </summary>
public sealed class RoundRuntimeState
{
    public required long RoundId { get; init; }
    public required decimal CrashPoint { get; init; }
    public decimal CurrentMultiplier { get; internal set; } = 1.00m;
    public DateTimeOffset StartsAt { get; init; }
    public bool IsCrashed { get; internal set; }
    public long TickSequence { get; internal set; }
}

/// <summary>
/// Immutable copy used when a round update crosses service/thread boundaries.
/// A delayed command can therefore never accidentally read a newer round.
/// </summary>
public sealed record RoundRuntimeSnapshot(
    long RoundId,
    decimal CurrentMultiplier,
    DateTimeOffset StartsAt,
    bool IsCrashed,
    long TickSequence);
