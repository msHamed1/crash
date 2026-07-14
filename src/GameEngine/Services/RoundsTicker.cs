using System.Collections.Concurrent;
using Crash.Domain.Contracts.Commands;
using Crash.Domain.Options;
using Crash.Domain.State;

namespace GameEngine.Services;

 
public sealed class RoundsTicker:BackgroundService
{
    
    private readonly ILogger<RoundsTicker> _logger;
     // The authoritative multiplier is e^(rate * elapsedSeconds), not the number of ticker iterations.
    private const double GrowthRatePerSecond = 0.12d;

    private readonly GameEngineOptions _options;
    private readonly RoundsService  _roundsService;

    private static readonly TimeSpan EvaluationInterval =
        TimeSpan.FromMilliseconds(50);

    private static readonly TimeSpan BroadcastInterval =
        TimeSpan.FromMilliseconds(100);

    private readonly ConcurrentDictionary<long, DateTimeOffset> _lastBroadcasts = new();



    public RoundsTicker( ILogger<RoundsTicker> _logger,GameEngineOptions _options, RoundsService _roundsService)
    {
        this._logger = _logger;
        this._options = _options;
        this._roundsService = _roundsService;

    }

    private bool ShouldBroadcast(long tableId, DateTimeOffset now)
    {
        var lastBroadcast = _lastBroadcasts.GetOrAdd(
            tableId,
            DateTimeOffset.MinValue);

        if (now - lastBroadcast < BroadcastInterval)
            return false;

        _lastBroadcasts[tableId] = now;
        return true;
    }

    private async Task SendCrashEvent(RoundRuntimeSnapshot round, long tableId, CancellationToken ct)
    {
        var envelop = new RoundCrashCommand
        {
            CurrentMultiplier = round.CurrentMultiplier,
            RoundId = round.RoundId.ToString(),
            TableId = tableId.ToString(),
            TickSequence = round.TickSequence

       };
       await _roundsService.EnqueueAsync(envelop, ct);
    }
    
    
    private async Task SendTickEvent(RoundRuntimeSnapshot round, long tableId, CancellationToken ct)
    {
        var envelop = new RoundTickCommand 
        {
            CurrentMultiplier = round.CurrentMultiplier,
            RoundId = round.RoundId.ToString(),
            TableId = tableId.ToString(),
            TickSequence = round.TickSequence

        };
        await _roundsService.EnqueueAsync(envelop, ct);
       
        
    }

  
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var key in _options.Tables)
            {
                var table = key.Value;
                try
                {
                    // Isolate each table so one invalid round cannot terminate the ticker
                    // and stop crash detection for every table owned by this engine.
                    if (!table.TryAdvanceRound(now, GrowthRatePerSecond, out var round, out var justCrashed) || round is null)
                        continue;

                    if (justCrashed)
                    {
                        // Crash lifecycle events must bypass normal tick throttling.
                        await SendCrashEvent(round, table.TableId, stoppingToken);
                        _lastBroadcasts.TryRemove(table.TableId, out _);
                        _logger.LogInformation(
                            "Round {RoundId} crashed at {Multiplier}",
                            round.RoundId,
                            round.CurrentMultiplier);
                        continue;
                    }

                    if (!ShouldBroadcast(table.TableId, now)) continue;

                    await SendTickEvent(round, table.TableId, stoppingToken);
                    _logger.LogDebug(
                        "Round {RoundId} tick {Multiplier} sequence {TickSequence}",
                        round.RoundId,
                        round.CurrentMultiplier,
                        round.TickSequence);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    _logger.LogError(
                        exception,
                        "Failed to evaluate active round for table {TableId}.",
                        table.TableId);
                }

            }
            await Task.Delay(EvaluationInterval, stoppingToken);


        }
    }
}
