using Crash.Domain.Contracts.Commands;
using Crash.Domain.Options;
using Crash.Domain.State;

namespace GameEngine.Services;

 
public sealed class RoundsTicker:BackgroundService
{
    
    private readonly ILogger<RoundsTicker> _logger;
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(50);
    // The authoritative multiplier is e^(rate * elapsedSeconds), not the number of ticker iterations.
    private const double GrowthRatePerSecond = 0.12d;

    private readonly GameEngineOptions _options;
    private readonly RoundsService  _roundsService;

    private readonly object _lock = new();

    
    public RoundsTicker( ILogger<RoundsTicker> _logger,GameEngineOptions _options, RoundsService _roundsService)
    {
        this._logger = _logger;
        this._options = _options;
        this._roundsService = _roundsService;

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
                if (!table.TryAdvanceRound(now, GrowthRatePerSecond, out var round, out var justCrashed) || round is null)
                    continue;

                if (justCrashed)
                {
                    await SendCrashEvent(round, table.TableId, stoppingToken);
                    _logger.LogInformation(
                        "Round {RoundId} crashed at {Multiplier}",
                        round.RoundId,
                        round.CurrentMultiplier);
                    continue;
                }

                await SendTickEvent(round, table.TableId, stoppingToken);
                _logger.LogInformation(
                    "Round {RoundId} tick {Multiplier} sequence {TickSequence}",
                    round.RoundId,
                    round.CurrentMultiplier,
                    round.TickSequence);

            }
            await Task.Delay(TickInterval, stoppingToken);


        }
    }
}
