namespace Crash.Domain.Contracts.Consumer;

public sealed record MessageHeader
{
    public long MessageId { get; init; }
    public string MessageType { get; init; } = default!;
    public long TableId { get; init; }
    public string CorrelationId { get; init; } = default!;
}