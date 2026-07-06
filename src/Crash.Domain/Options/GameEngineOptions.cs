using System.Collections.Concurrent;

namespace Crash.Domain.Options;

public sealed class GameEngineOptions
{
    public const string SectionName = "GameEngine";
    public long OwnerId { get; set; }

    public string EngineId { get; init; } = "engine-1";
    public ConcurrentDictionary<long, long> tokensPerTable { get; set; } = new();
    
    
}
