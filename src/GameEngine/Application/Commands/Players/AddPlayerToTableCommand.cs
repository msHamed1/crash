namespace GameEngine.Application.Commands.Players;

public sealed record AddPlayerToTableCommand : GameCommand
{
    public required string PlayerId { get; init; }
    public override string MessageType => "Joined";
}
