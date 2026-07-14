using Crash.Domain.Entities;
using Crash.Persistence;
using Crash.Persistence.Logger;

namespace GameEngine.Services;

public class DataBaseLogWriter :BackgroundService
{
    
    private const int BatchSize = 200;
    private static readonly TimeSpan FlushInterval =
        TimeSpan.FromSeconds(1);
    
    
    private readonly DatabaseLogQueue _queue;

    private readonly IServiceScopeFactory _scopeFactory;

    private readonly ILogger<DataBaseLogWriter> _fallbackLogger;

    
    
    public DataBaseLogWriter(
        DatabaseLogQueue queue,
        IServiceScopeFactory scopeFactory,
        ILogger<DataBaseLogWriter> fallbackLogger)
    {
        _queue = queue;
        _scopeFactory = scopeFactory;
        _fallbackLogger = fallbackLogger;
    }
   protected override async Task ExecuteAsync(
    CancellationToken stoppingToken)
{
    var batch = new List<AppLog>(BatchSize);
    using var timer = new PeriodicTimer(FlushInterval);

    var logEnumerator = _queue
        .ReadAllAsync(stoppingToken)
        .GetAsyncEnumerator(stoppingToken);

    try
    {
        var readTask = logEnumerator.MoveNextAsync().AsTask();
        var timerTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();

        while (!stoppingToken.IsCancellationRequested)
        {
            var completedTask = await Task.WhenAny(
                readTask,
                timerTask);

            if (completedTask == readTask)
            {
                if (!await readTask)
                    break;

                batch.Add(logEnumerator.Current);

                if (batch.Count >= BatchSize)
                    await FlushAsync(batch, stoppingToken);

                readTask = logEnumerator.MoveNextAsync().AsTask();
            }

            if (completedTask == timerTask)
            {
                if (await timerTask && batch.Count > 0)
                    await FlushAsync(batch, stoppingToken);

                timerTask = timer
                    .WaitForNextTickAsync(stoppingToken)
                    .AsTask();
            }
        }
    }
    catch (OperationCanceledException)
        when (stoppingToken.IsCancellationRequested)
    {
    }
    finally
    {
        await logEnumerator.DisposeAsync();

        if (batch.Count > 0)
        {
            try
            {
                await FlushAsync(batch, CancellationToken.None);
            }
            catch
            {
            }
        }
    }
}

private async Task FlushAsync(
    List<AppLog> batch,
    CancellationToken cancellationToken)
{
    var logsToPersist = batch.ToArray();
    batch.Clear();

    await PersistBatchAsync(
        logsToPersist,
        cancellationToken);
}

private async Task PersistBatchAsync(
    IReadOnlyCollection<AppLog> logs,
    CancellationToken cancellationToken)
{
    try
    {
        await using var scope =
            _scopeFactory.CreateAsyncScope();

        var db = scope.ServiceProvider
            .GetRequiredService<DataContext>();

        await db.AppLogs.AddRangeAsync(
            logs,
            cancellationToken);

        await db.SaveChangesAsync(cancellationToken);
    }
    catch (Exception exception)
    {
        Console.Error.WriteLine(
            $"Failed to persist {logs.Count} logs: {exception}");
    }
}
}