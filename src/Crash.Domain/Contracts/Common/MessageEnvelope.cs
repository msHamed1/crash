namespace Crash.Domain.Contracts.Common;

public abstract record MessageEnvelope<TData>
{
    protected MessageEnvelope()
    {
    }

    protected MessageEnvelope(
        string correlationId,
        DateTime createdAtUtc,
        long tableId,
        DateTime? processedAtGatewayUtc,
        DateTime? processedAtClientUtc,
        TData data)
    {
        CorrelationId = correlationId;
        CreatedAtUtc = createdAtUtc;
        TableId = tableId;
        ProcessedAtGatewayUtc = processedAtGatewayUtc;
        ProcessedAtClientUtc = processedAtClientUtc;
        Data = data;
    }
    
    public abstract string MessageType { get; }

    public DateTime CreatedAtUtc { get; init; } = DateTime.UtcNow;

    public DateTime? ProcessedAtGatewayUtc { get; init; }
    public DateTime? ProcessedAtEngineUtc { get; init; }
    public DateTime? ProcessedAtClientUtc { get; init; }

    public long TableId { get; init; }

    public string CorrelationId { get; init; } = default!;

    public TData Data { get; init; } = default!;
}