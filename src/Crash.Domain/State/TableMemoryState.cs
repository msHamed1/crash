using System.Collections.Concurrent;

namespace Crash.Domain.State;


/// <summary>
/// A singleton in-memory store that holds the runtime state for every table
/// currently owned by this Game Engine instance.
///
/// This class is the central source of runtime state shared between background
/// services such as:
/// - RoundEngine
/// - RoundTicker
/// - PlayerMessageConsumer
/// - SignalR publishers
///
/// The data stored here is temporary and exists only while the process is running.
/// It is never considered the source of truth. Persistent state is stored in the database.
/// </summary>
public sealed class TableMemoryState
{
    private readonly ConcurrentDictionary<long, TableRuntimeState> _tables = new();

    public TableRuntimeState GetOrCreateTable(long tableId)
    {
        return _tables.GetOrAdd(tableId, id => new TableRuntimeState(id));
    }

    public bool TryGetTable(long tableId, out TableRuntimeState? state)
    {
        return _tables.TryGetValue(tableId, out state);
    }

    public bool RemoveTable(long tableId)
    {
        return _tables.TryRemove(tableId, out _);
    }
}