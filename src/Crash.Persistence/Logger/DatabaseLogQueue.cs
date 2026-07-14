using System.Threading.Channels;
using Crash.Domain.Entities;

namespace Crash.Persistence.Logger;

public class DatabaseLogQueue
{

    private readonly Channel<AppLog> _channel;

    public DatabaseLogQueue(int capacity =10_000)
    {
        _channel = Channel.CreateBounded<AppLog>(
            new BoundedChannelOptions(capacity)
            {
                 SingleReader = false,
                SingleWriter = false,
                // Logging must never block the game engine.
                FullMode = BoundedChannelFullMode.DropOldest
            });
    }
    
    
    public bool TryEnqueue(AppLog log)
    {
        return _channel.Writer.TryWrite(log);
    }
    
    public IAsyncEnumerable<AppLog> ReadAllAsync(
        CancellationToken cancellationToken)
    {
        return _channel.Reader.ReadAllAsync(cancellationToken);
    }
}