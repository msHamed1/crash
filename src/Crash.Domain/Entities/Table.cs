namespace Crash.Domain.Entities;

public class Table :BaseEntity
{
    public long Id { get; set; }

    public string TableName { get; set; } = null!;

    public long? OwnerId { get; set; }
    public Owner? Owner { get; set; }

    public long FencingToken { get; set; } = 0;
    public DateTimeOffset? LeaseExpiresAt { get; set; }

    public List<Round> Rounds { get; set; } = new();

    
}
