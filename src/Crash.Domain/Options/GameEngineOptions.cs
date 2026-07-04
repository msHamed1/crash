namespace Crash.Domain.Options;

public sealed class GameEngineOptions
{
    public const string SectionName = "GameEngine";

    public string EngineId { get; init; } = "engine-1";
    public string[] TableIds { get; init; } = ["default-table"];
}
