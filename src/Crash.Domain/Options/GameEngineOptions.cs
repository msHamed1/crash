using System.Collections.Concurrent;
using Crash.Domain.State;

namespace Crash.Domain.Options;

public sealed class GameEngineOptions
{
    public const string SectionName = "GameEngine";
    public long OwnerId { get; set; }

    public string EngineId { get; init; } = "engine-1";
    
    /// <summary>
    /// Runtime state for every table currently owned by this engine.
    /// </summary>
    public ConcurrentDictionary<long, TableRuntimeState> Tables { get; } = new();
}
