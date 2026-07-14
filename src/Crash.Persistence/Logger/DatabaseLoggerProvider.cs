using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Crash.Persistence.Logger;

public class DatabaseLoggerProvider:ILoggerProvider
{
    private readonly DatabaseLogQueue _queue;

    public DatabaseLoggerProvider(DatabaseLogQueue queue)
    {
      _queue = queue;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new DatebaseLogger(categoryName, _queue);
    }
    public void Dispose()
    {
    }
}