namespace Crash.Persistence;

/// <summary>
/// Durable inbox entry used to make DB-worker message processing idempotent.
/// It is inserted in the same transaction as the corresponding bet write.
/// </summary>
public sealed class ProcessedDbMessage
{
    public Guid MessageId { get; set; }
    public required string MessageType { get; set; }
    public long TableId { get; set; }
    public long RoundId { get; set; }
    public long Sequence { get; set; }
    public DateTimeOffset ProcessedAt { get; set; }
}
