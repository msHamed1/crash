using System.Collections.Concurrent;
using Crash.Domain.Entities;

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
    // Immutable round identity and fairness configuration.
    public required long RoundId { get; init; }

    // Keep this server-side. The crash point must not be exposed to clients
    // before the round has completed.
    public required decimal CrashPoint { get; init; }

    public required DateTimeOffset StartsAt { get; init; }

    // Mutable round state may only be changed through the table's synchronized
    // state-management methods.
    public decimal CurrentMultiplier { get; internal set; } = 1.00m;

    public bool IsCrashed { get; internal set; }

    // Monotonically increasing state version used to reject stale or
    // out-of-order tick messages.
    public long TickSequence { get; internal set; }

    // One accepted bet per player for this round.
    // TableRuntimeState owns synchronization when accessing this dictionary.
    private readonly Dictionary<long, Bet> _betsByPlayer = new();

    public bool HasBet(long playerId)
    {
        return _betsByPlayer.ContainsKey(playerId);
    }

    internal bool TryAddBet(Bet bet)
    {
        return _betsByPlayer.TryAdd(bet.PlayerId, bet);
    }

    internal bool TryRemoveBet(Bet bet)
    {
        return _betsByPlayer.TryGetValue(bet.PlayerId, out var currentBet)
               && currentBet.BetId == bet.BetId
               && _betsByPlayer.Remove(bet.PlayerId);
    }

    public Bet? GetBet(long playerId)
    {
        return _betsByPlayer.GetValueOrDefault(playerId);
    }

    public IReadOnlyCollection<Bet> GetBetsSnapshot()
    {
        // Return a snapshot so callers cannot mutate the authoritative
        // collection or enumerate it while it is being changed.
        return _betsByPlayer.Values.ToArray();
    }
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
