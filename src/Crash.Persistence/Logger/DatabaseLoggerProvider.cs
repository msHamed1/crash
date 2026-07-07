using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Crash.Persistence.Migrations;

public class DatabaseLoggerProvider:ILoggerProvider
{
    private readonly IServiceScopeFactory scopeFactory;

    public DatabaseLoggerProvider(IServiceScopeFactory scopeFactory)
    {
        this.scopeFactory = scopeFactory;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new DatebaseLogger(categoryName, scopeFactory);
    }

    public void Dispose()
    {
    }
}