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

            var elapsedSeconds = Math.Max(0, (now - _currentRound.StartsAt).TotalSeconds);
            var calculated = (decimal)Math.Exp(growthRatePerSecond * elapsedSeconds);
            var multiplier = Math.Floor(calculated * 100m) / 100m;

            // Do not flood RabbitMQ with identical two-decimal snapshots.
            if (multiplier < _currentRound.CrashPoint && multiplier == _currentRound.CurrentMultiplier)
                return false;

            _currentRound.TickSequence++;
            if (multiplier >= _currentRound.CrashPoint)
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
