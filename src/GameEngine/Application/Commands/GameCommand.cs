namespace GameEngine.Application.Commands;

/// <summary>
/// Internal GameEngine instruction. These types never cross RabbitMQ or WebSocket boundaries.
/// </summary>
public abstract record GameCommand
{
    public abstract string MessageType { get; }
    public string TableId { get; init; } = string.Empty;
    public string RoundId { get; init; } = string.Empty;
}
