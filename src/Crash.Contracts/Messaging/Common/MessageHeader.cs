namespace Crash.Contracts.Messaging.Common;

/// Minimal RabbitMQ envelope fields used to route a message before deserializing its payload.
public sealed record MessageHeader
{
    public string? MessageId { get; init; }
    public required string MessageType { get; init; }
    public long TableId { get; init; }
    public required string CorrelationId { get; init; }
}
