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

    public RoundRuntimeState? CurrentRound { get; private set; }

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
            CurrentRound = round;
        }
    }

    public void ClearCurrentRound()
    {
        lock (_lock)
        {
            CurrentRound = null;
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
} 