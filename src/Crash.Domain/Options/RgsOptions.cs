namespace Crash.Domain.Options;

public sealed class RgsOptions
{
    public const string SectionName = "Rgs";
    public long OwnerId { get; set; }

    public string EngineId { get; init; } = "engine-1";
 

}