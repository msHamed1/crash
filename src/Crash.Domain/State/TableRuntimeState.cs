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


    private static RoundRuntimeSnapshot Snapshot(RoundRuntimeState round) => new(
        round.RoundId,
        round.CurrentMultiplier,
        round.StartsAt,
        round.IsCrashed,
        round.TickSequence);
}
