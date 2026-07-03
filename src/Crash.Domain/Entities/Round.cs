namespace Crash.Domain.Entities;

public class Round
{
    public string Id { get; set; } = null!;

    public int TableId { get; set; }
    public Table Table { get; set; } = null!;

    public string? OwnerId { get; set; }
    public long FencingToken { get; set; }
    public DateTimeOffset LeaseExpiresAt { get; set; }

    public RoundStatus Status { get; set; }

    public DateTimeOffset StartTime { get; set; }
    public DateTimeOffset? EndTime { get; set; }

    public List<Player> Players { get; set; } = new();
    public List<Bet> Bets { get; set; } = new();

    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? UpdatedAt { get; set; }
}

public enum RoundStatus
{
    Pending = 0,
    Betting = 1,
    Running = 2,
    Crashed = 3,
    Settling = 4,
    Settled = 5,
    Cancelled = 6
}

// Table.OwnerId + Table.FencingToken
// controls table loop, round creation, pause/resume, table broadcast
//
// Round.OwnerId + Round.FencingToken
// controls bets, cashouts, crash result, settlement

// UPDATE Rounds
// SET Status = 'Settled',
//     EndTime = @now
// WHERE Id = @roundId
// AND OwnerId = @ownerId
// AND FencingToken = @fencingToken
// AND Status = 'Settling';
