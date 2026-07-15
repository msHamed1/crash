namespace GameEngine.Application.Commands.Rounds;

public sealed record CreateRoundCommand : GameCommand
{
    public override string MessageType => "New";
}
