using System.Collections.Concurrent;
using Crash.Domain.Entities;
using Crash.Domain.State.Settlement;

namespace Crash.Domain.State;



/// <summary>
/// Represents the complete runtime state for a single game table.
///
/// This object is kept entirely in memory and contains the mutable state
/// required while a table is active.
///
/// Examples:
/// - Connected players
/// - Current fencing token
/// - Current running round
///
/// This object is designed to be shared between multiple background services.
/// </summary>
public sealed class TableRuntimeState
{
    private readonly object _lock = new();
 
    public long TableId { get; }

    public long FencingToken { get; private set; }

    private RoundRuntimeState? _currentRound;

    private readonly List<PlayerRuntimeState> _players = new();
    
    

    public TableRuntimeState(long tableId)
    {
        TableId = tableId;
    }

    public Bet? AddNewBet(
        PlayerRuntimeState player,
        decimal amount,
        long roundId,
        string betId,
        string currency,
        decimal? autoCashoutMultiplier,
        bool? autoCashoutEnabled
        )
    {
        const decimal minimumBet = 0.10m;

        // Validate values that do not depend on mutable round state
        // before acquiring the table lock.
        if (amount < minimumBet)
        {
            return null;
        }

        lock (_lock)
        {
            var round = _currentRound;

            if (round is null || round.IsCrashed)
            {
                return null;
            }

            // A client may submit a delayed command from a previous round.
            // Always compare it with the table's authoritative current round.
            if (round.RoundId != roundId)
            {
                return null;
            }

            var now = DateTimeOffset.UtcNow;
            var bettingClosesAt = round.StartsAt.AddSeconds(-1);

            // This rejects bets during the final second before launch
            // and all bets arriving after the round has started.
            if (now >= bettingClosesAt)
            {
                return null;
            }

            if (player.Balance < amount)
            {
                return null;
            }
           // Reserve the runtime balance only after the duplicate-bet check succeeds.
            player.Balance -= amount;
            var bet = new Bet
            {
                PlayerId = player.PlayerId,
                RoundId = round.RoundId,
                StakeAmount = amount,
                AutoCashoutEnabled =autoCashoutEnabled,
                AutoCashoutMultiplier = autoCashoutMultiplier,
                // The correlation ID is stable across retries and is protected by a unique DB index.
                BetId = betId,
                Currency = currency,
                IsPersisted =  false,
                Status = BetStatus.Placed,
                CreatedAt = now,
                Player =
                {
                    BalanceInUSD = player.Balance,
                    ExternalId = player.ExternalId,
                    Id =  player.PlayerId,
                    TableId =  TableId,
                    
                },
            };

            // TryAdd is the authoritative duplicate-bet check.
            // It remains safe even if the preliminary ContainsKey check is removed.
            if (!round.TryAddBet(bet))
                return null;

            
            return bet;
        }
    }
    
    public void ApplyCommittedCashout(
        string betId,
        decimal cashoutMultiplier,
        decimal payoutAmount,
        decimal updatedBalance,
        DateTimeOffset settledAt)
    {
        lock (_lock)
        {
            var bet = _currentRound?
                .GetBetsSnapshot()
                .FirstOrDefault(bet => bet.BetId == betId);

            if (bet is null)
                return;

            bet.Status = BetStatus.CashedOut;
            bet.CashedOutAtMultiplier = cashoutMultiplier;
            bet.CashedOutAt = settledAt;
            bet.SettledAt = settledAt;
            bet.PayoutAmount = payoutAmount;
            bet.Pl = payoutAmount - bet.StakeAmount;

            var player = _players.FirstOrDefault(
                player => player.PlayerId == bet.PlayerId);

            if (player is not null)
            {
                // Copy the authoritative committed database balance.
                player.Balance = updatedBalance;
            }
        }
    }

    public bool RollbackBetInMemory(Bet bet, PlayerRuntimeState player)
    {
        lock (_lock)
        {
           // Never let a delayed rollback remove this player's bet from a newer round.
           if (_currentRound?.RoundId != bet.RoundId)
               return false;

           var removed = _currentRound.TryRemoveBet(bet);
           if (removed)
           {
               player.Balance += bet.StakeAmount;
               return true;

           }

           return false;



        }
    }

    public Bet? GetPlayerBet(long roundId, long playerId)
    {
        lock (_lock)
        {
            return _currentRound?.RoundId == roundId
                ? _currentRound.GetBet(playerId)
                : null;
        }
    }

    public void SetPlayerBalance(PlayerRuntimeState player, decimal balance)
    {
        lock (_lock)
        {
            // The committed database value is authoritative after placement succeeds.
            player.Balance = balance;
        }
    }
 
    public IReadOnlyList<PlayerRuntimeState> Players
    {
        get
        {
            lock (_lock)
            {
                return _players.ToList();
            }
        }
    }

    public void SetFencingToken(long fencingToken)
    {
        lock (_lock)
        {
            FencingToken = fencingToken;
        }
    }

    public void SetCurrentRound(RoundRuntimeState round)
    {
        lock (_lock)
        {
            _currentRound = round;
        }
    }

    public RoundRuntimeSnapshot? GetCurrentRoundSnapshot()
    {
        lock (_lock)
        {
            return _currentRound is null ? null : Snapshot(_currentRound);
        }
    }
    
    public RoundRuntimeState? GetCurrentRound()
    {
        lock (_lock)
        {
            return _currentRound is null ? null :_currentRound;
        }
    }
    ///This returns immutable information. It should not settle or change balances.
    public IReadOnlyList<BetSettlementCandidate>
        GetAutoCashoutCandidates(
            long roundId,
            decimal currentMultiplier)
    {
        lock (_lock)
        {
            if (_currentRound?.RoundId != roundId)
                return [];

            return _currentRound
                .GetBetsSnapshot()
                .Where(bet =>
                    bet.Status == BetStatus.Accepted &&
                    bet.AutoCashoutEnabled == true &&
                    bet.AutoCashoutMultiplier.HasValue &&
                    currentMultiplier >=
                    bet.AutoCashoutMultiplier.Value)
                .Select(bet => new BetSettlementCandidate(
                    BetId: bet.BetId,
                    PlayerId: bet.PlayerId,
                    RoundId: bet.RoundId,
                    StakeAmount: bet.StakeAmount,
                    CashoutMultiplier:
                    bet.AutoCashoutMultiplier!.Value))
                .ToArray();
        }
    }
    
    ///Losing bets do not increase player balance.
    public void ApplyCommittedLoss(
        string betId,
        DateTimeOffset settledAt)
    {
        lock (_lock)
        {
            var bet = _currentRound?
                .GetBetsSnapshot()
                .FirstOrDefault(bet => bet.BetId == betId);

            if (bet is null)
                return;

            bet.Status = BetStatus.Lost;
            bet.PayoutAmount = 0;
            bet.Pl = -bet.StakeAmount;
            bet.SettledAt = settledAt;
        }
    }

    public Bet? TryCancelBet(  long playerId, long roundId)
    {
        lock (_lock)
        {
            
            if (_currentRound?.RoundId != roundId)
            {
                return null;
            }
            GetPlayer(playerId,player: out var player);
            if (player is null)
            {
                
                    return null;
                 
            }

            var bet = GetPlayerBet(roundId, playerId);
            if (bet is null)
            {
                return null;
            }
          var betIsCanceled=  RollbackBetInMemory(bet, player);
            
          return betIsCanceled ? null : bet;
            

        }
    }
    
    public Bet? SetBetIsPersisted(  long playerId, long roundId)
    {
        lock (_lock)
        {
            
            if (_currentRound?.RoundId != roundId)
            {
                return null;
            }
            GetPlayer(playerId,player: out var player);
            if (player is null)
            {
                
                return null;
                 
            }

            var bet = GetPlayerBet(roundId, playerId);
            if (bet is null)
            {
                return null;
            }
            bet.IsPersisted = true;
            
            return bet;
            

        }
    }

    /// <summary>
    /// Advances the round from elapsed server time while holding the table lock.
    /// Delayed ticker iterations cannot slow down or alter the multiplier curve.
    /// </summary>
    public bool TryAdvanceRound(
        DateTimeOffset now,
        double growthRatePerSecond,
        out RoundRuntimeSnapshot? snapshot,
        out bool justCrashed)
    {
        lock (_lock)
        {
            snapshot = null;
            justCrashed = false;

            if (_currentRound is null || _currentRound.IsCrashed || now < _currentRound.StartsAt)
                return false;

            if (!double.IsFinite(growthRatePerSecond) || growthRatePerSecond <= 0)
                throw new ArgumentOutOfRangeException(
                    nameof(growthRatePerSecond),
                    "The multiplier growth rate must be finite and greater than zero.");

            if (_currentRound.CrashPoint < 1.00m)
                throw new InvalidOperationException(
                    $"Round {_currentRound.RoundId} has invalid crash point {_currentRound.CrashPoint}.");

            var elapsedSeconds = Math.Max(0, (now - _currentRound.StartsAt).TotalSeconds);
            var exponent = growthRatePerSecond * elapsedSeconds;

            // Compare in logarithmic space before calling Exp or converting to decimal.
            // A stale round may have been running for minutes; e^x can then exceed
            // decimal.MaxValue and must be treated as having crossed its crash point.
            var crashExponent = Math.Log((double)_currentRound.CrashPoint);
            var reachedCrashPoint = !double.IsFinite(exponent) || exponent >= crashExponent;

            decimal multiplier;
            if (reachedCrashPoint)
            {
                multiplier = _currentRound.CrashPoint;
            }
            else
            {
                // This conversion is safe because the calculated value is lower than the
                // decimal CrashPoint. Round down so clients never see a value above the server.
                var calculated = Math.Exp(exponent);
                multiplier = (decimal)(Math.Floor(calculated * 100d) / 100d);
            }

            // Do not flood RabbitMQ with identical two-decimal snapshots.
            if (multiplier < _currentRound.CrashPoint && multiplier == _currentRound.CurrentMultiplier)
                return false;

            _currentRound.TickSequence++;
            if (reachedCrashPoint)
            {
                _currentRound.CurrentMultiplier = _currentRound.CrashPoint;
                _currentRound.IsCrashed = true;
                justCrashed = true;
            }
            else
            {
                _currentRound.CurrentMultiplier = Math.Max(1.00m, multiplier);
            }

            snapshot = Snapshot(_currentRound);
            return true;
        }
    }

    public void ClearCurrentRound()
    {
        lock (_lock)
        {
            _currentRound = null;
        }
    }

    public bool AddPlayer(PlayerRuntimeState player)
    {
        lock (_lock)
        {
            if (_players.Any(p => p.PlayerId == player.PlayerId))
                return false;

            _players.Add(player);
            return true;
        }
    }
    
    public bool GetPlayer(long  playerId, out PlayerRuntimeState? player)
    {
        lock (_lock)
        {

            player= _players.FirstOrDefault(p => p.PlayerId == playerId);
            return player is not null;
        }
    }

    public bool RemovePlayer(long playerId)
    {
        lock (_lock)
        {
            var player = _players.FirstOrDefault(p => p.PlayerId == playerId);

            if (player is null)
                return false;

            _players.Remove(player);
            return true;
        }
    }
    
    
    public IReadOnlyList<Bet> CancelUnpersistedBetsBeforeStart(long roundId)
    {
        lock (_lock)
        {
            if (_currentRound?.RoundId != roundId)
                return [];

            var now = DateTimeOffset.UtcNow;
            var bettingClosesAt = _currentRound.StartsAt.AddSeconds(-1);

            if (now < bettingClosesAt)
                return [];

            var cancelled = new List<Bet>();

            foreach (var bet in _currentRound.GetBetsSnapshot()
                         .Where(b => !b.IsPersisted && b.Status == BetStatus.Placed))
            {
                var player = _players.FirstOrDefault(p => p.PlayerId == bet.PlayerId);
                if (player is null)
                    continue;

                if (_currentRound.TryRemoveBet(bet))
                {
                    player.Balance += bet.StakeAmount;
                    bet.Status = BetStatus.Canceled;
                    cancelled.Add(bet);
                }
            }

            return cancelled;
        }
    }

    private static RoundRuntimeSnapshot Snapshot(RoundRuntimeState round) => new(
        round.RoundId,
        round.CurrentMultiplier,
        round.StartsAt,
        round.IsCrashed,
        round.TickSequence);
}
