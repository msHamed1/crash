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

    public List<Bet> SettleCurrentRoundBetsIfNeeded()
    {
        List<Bet> bets = new List<Bet>();
        foreach (var bet in _betsByPlayer.Select(keyValuePair => keyValuePair.Value).Where(bet => bet.Status == BetStatus.Accepted))
        {
            if (bet.AutoCashoutEnabled ==true)
            {
                // Auto cashout enabled
                var updatedBet=    AutoSettleBetIfNeeded(bet);
                if (updatedBet != null)
                {
                    bets.Add(updatedBet);
                    continue;
                }
                
               
              
            }

            if (!IsCrashed) continue;
            var lostBet=   AutoSettleBetIfRoundCrashed(bet);
            bets.Add(lostBet);
        }

        return bets;
    }
    private Bet AutoSettleBetIfRoundCrashed(Bet bet)
    {
         
        bet.SettledAt = DateTimeOffset.Now;
        var winAmount =0;
        bet.Pl =bet.StakeAmount -winAmount ;
        bet.Status =  BetStatus.Lost;
        bet.PayoutAmount = winAmount;

        return bet;

    }
    private Bet? AutoSettleBetIfNeeded(Bet bet)
    {
        
      var  reachedTarget =
            bet.AutoCashoutMultiplier < CrashPoint &&
            CurrentMultiplier >= bet.AutoCashoutMultiplier;
        // Multiplier may jump from 1.49 -> 1.51, so settle when we've reached or exceeded the target.
        if (bet.AutoCashoutMultiplier is null ||reachedTarget)
            return null;
        var now = DateTimeOffset.UtcNow;

        bet.SettledAt =now ;

        var multiplier = bet.AutoCashoutMultiplier.Value;

        var winAmount = bet.StakeAmount * multiplier;

        bet.Pl = winAmount - bet.StakeAmount;
        bet.PayoutAmount = winAmount;
        bet.CashedOutAtMultiplier = multiplier;
        bet.CashedOutAt = now ;

        bet.Status = BetStatus.CashedOut;

        return bet;
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
