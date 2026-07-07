using Crash.Domain.Entities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Crash.Persistence.Migrations;

public sealed class DatebaseLogger :ILogger
{
    private readonly string _category;

    private readonly IServiceScopeFactory _scopeFactory;

    public DatebaseLogger(string category, IServiceScopeFactory scopeFactory)
    {
        _category = category;
        _scopeFactory = scopeFactory;
    }
    
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        
        var message = formatter(state, exception);
        
        try
        {
            using var scope = _scopeFactory.CreateScope();

            var db = scope.ServiceProvider.GetRequiredService<DataContext>();
            db.AppLogs.Add(new AppLog
            {
                Category = _category,
                CreatedAt = DateTime.UtcNow,
                Message = message,
                Exception = exception?.ToString(),
                Level = logLevel.ToString(),
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
        return true;
        //return logLevel >= LogLevel.Warning && logLevel != LogLevel.None;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }
}
