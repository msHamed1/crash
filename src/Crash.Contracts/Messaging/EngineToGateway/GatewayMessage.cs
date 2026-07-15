namespace Crash.Contracts.Messaging.EngineToGateway;

/// <summary>
/// Base contract published by GameEngine for delivery through RealtimeGateway.
/// </summary>
public abstract record GatewayMessage
{
    public abstract string MessageType { get; }
    public long TableId { get; init; }
    public required string MessageId { get; init; }
}
