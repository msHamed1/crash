namespace Crash.Domain.Contracts.RoundMessages;

public sealed record TimingConfig
{
    public int NewRoundToRoundStartDelayInMs { get; init; }
    public int AwaitingBetsSubmissionToOperatorsInMs { get; init; }
    public int ResultDisplayDelayInMs { get; init; }
    public int EmitMultiplierDelayInMs { get; init; }

    public Dictionary<int, double> MultiplierSpeedByElapsedTimeInMs { get; init; } = [];
}