namespace GameEngine.Entities;

public class Table
{
    public int Id { get; set; }

    public string TableName { get; set; } = null!;

    public string? OwnerId { get; set; }
    public long FencingToken { get; set; }
    public DateTimeOffset LeaseExpiresAt { get; set; }

    public List<Round> Rounds { get; set; } = new();

    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? UpdatedAt { get; set; }
}