using System.Text.Json;
using Crash.Domain.Entities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Crash.Persistence.Logger;

public sealed class DatebaseLogger :ILogger
{
    private readonly string _category;

    private readonly IServiceScopeFactory _scopeFactory;

    public DatebaseLogger(string category, IServiceScopeFactory scopeFactory)
    {
        _category = category;
        _scopeFactory = scopeFactory;
    }
    
    public void  Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        
        var message = formatter(state, exception);
        string? data = null;

        
        try
        {
            if (state is IEnumerable<KeyValuePair<string, object?>> values)
            {
                var obj = values
                    .Where(x => x.Key != "{OriginalFormat}")
                    .ToDictionary(x => x.Key, x => x.Value);

                if (obj.Count > 0)
                {
                    data = JsonSerializer.Serialize(obj);
                }
            }
            using var scope = _scopeFactory.CreateScope();

            var db = scope.ServiceProvider.GetRequiredService<DataContext>();
            db.AppLogs.Add(new AppLog
            {
                Category = _category,
                CreatedAt = DateTime.UtcNow,
                Message = message,
                Exception = exception?.ToString(),
                Level = logLevel.ToString(),
                Data = data,
            });
            db.SaveChanges();
        }
        catch
        {
            // Database logging must not break application startup or migrations.
        }
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        // Database writes are synchronous in this provider. Persisting Information/Debug logs
        // from the 50 ms round loop can block the game engine and must never be on the hot path.
        return logLevel >= LogLevel.Warning && logLevel != LogLevel.None;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }
}
