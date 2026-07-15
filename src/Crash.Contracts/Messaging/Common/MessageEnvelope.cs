namespace Crash.Contracts.Messaging.Common;

/// Shared metadata carried by RabbitMQ integration messages.
public abstract record MessageEnvelope<TPayload>
{
    public abstract string MessageType { get; }
    public DateTime CreatedAtUtc { get; init; } = DateTime.UtcNow;
    public DateTime? ProcessedAtGatewayUtc { get; init; }
    public DateTime? ProcessedAtEngineUtc { get; init; }
    public DateTime? ProcessedAtClientUtc { get; init; }
    public long TableId { get; init; }
    public required string CorrelationId { get; init; }
    public required TPayload Data { get; init; }
}
