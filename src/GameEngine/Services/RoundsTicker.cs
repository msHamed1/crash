using System.Collections.Concurrent;
using Crash.Domain.Options;
using Crash.Domain.State;
using GameEngine.Application.Commands.Bets;
using GameEngine.Application.Commands.Rounds;

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
        var command = new CrashRoundCommand
        {
            CurrentMultiplier = round.CurrentMultiplier,
            RoundId = round.RoundId.ToString(),
            TableId = tableId.ToString(),
            TickSequence = round.TickSequence

       };
       await _roundsService.EnqueueAsync(command, ct);
    }
    
    
    private async Task SendTickEvent(RoundRuntimeSnapshot round, long tableId, CancellationToken ct)
    {
        var command = new AdvanceRoundCommand
        {
            CurrentMultiplier = round.CurrentMultiplier,
            RoundId = round.RoundId.ToString(),
            TableId = tableId.ToString(),
            TickSequence = round.TickSequence

        };
        await _roundsService.EnqueueAsync(command, ct);
       
        
    }

  

  
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var key in _options.Tables)
            {
                var now = DateTimeOffset.UtcNow;

                var table = key.Value;
                try
                {
                    // Isolate each table so one invalid round cannot terminate the ticker
                    // and stop crash detection for every table owned by this engine.
                    if (!table.TryAdvanceRound(now, GrowthRatePerSecond, out var round, out var justCrashed) || round is null)
                        continue;

                    if (justCrashed)
                    {
                        // TODO Process all bets in Memory 
                        await _roundsService.EnqueueAsync(
                            new ProcessAutoCashoutsCommand
                            {
                                TableId = table.TableId.ToString(),
                                RoundId = round.RoundId.ToString(),
                                CurrentMultiplier =
                                    round.CurrentMultiplier
                            },
                            stoppingToken);
                        // Crash lifecycle events must bypass normal tick throttling.
                        await SendCrashEvent(round, table.TableId, stoppingToken);
                       
                        
                        _lastBroadcasts.TryRemove(table.TableId, out _);
                        _logger.LogInformation(
                            "Round {RoundId} crashed at {Multiplier}",
                            round.RoundId,
                            round.CurrentMultiplier);
                        continue;
                    }

                    //TODO Check current Bets if auto Cashout is enabled on the current multiplier .
                    await _roundsService.EnqueueAsync(
                        new ProcessAutoCashoutsCommand
                        {
                            TableId = table.TableId.ToString(),
                            RoundId = round.RoundId.ToString(),
                            CurrentMultiplier =
                                round.CurrentMultiplier
                        },
                        stoppingToken);

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
