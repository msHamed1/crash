using Crash.Domain.Contracts.Commands;
using Crash.Domain.Options;
using Crash.Domain.State;

namespace GameEngine.Services;

 
public sealed class RoundsTicker:BackgroundService
{
    
    private readonly ILogger<RoundsTicker> _logger;
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(50);

    private readonly GameEngineOptions _options;
    private readonly RoundsService  _roundsService;

    private readonly object _lock = new();

    
    public RoundsTicker( ILogger<RoundsTicker> _logger,GameEngineOptions _options, RoundsService _roundsService)
    {
        this._logger = _logger;
        this._options = _options;
        this._roundsService = _roundsService;

    }

    private async Task SendCrashEvent(RoundRuntimeState roundTickMap,long TableId)
    {
        var envelop = new RoundCrashCommand
        {
            CurrentMultiplier = roundTickMap.CurrentMultiplier,
            RoundId = roundTickMap.RoundId.ToString(),
            TableId = TableId.ToString(),

        };
       await _roundsService.EnqueueAsync(envelop);

       var newRoundCommand = new NewRoundCommand()
       {
           TableId =TableId.ToString(),
       };
       await _roundsService.EnqueueAsync(newRoundCommand);
       
        
    }
    
    
    private async Task SendTickEvent(RoundRuntimeState roundTickMap,long TableId)
    {
        var envelop = new RoundTickCommand 
        {
            CurrentMultiplier = roundTickMap.CurrentMultiplier,
            RoundId = roundTickMap.RoundId.ToString(),
            TableId =TableId.ToString() ,

        };
        await _roundsService.EnqueueAsync(envelop);
       
        
    }

  
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var key  in _options.Tables)
            {
                var table = key.Value;

                var round = table.CurrentRound;
                if(round is null) continue;
                
                if (round.StartsAt > DateTimeOffset.UtcNow)
                    continue;

                if (round.IsCrashed)
                {
                    _logger.LogInformation(
                        $"Round {round.RoundId} is crashed at {round.CurrentMultiplier}",
                        round.RoundId,
                        round);
                    continue;
                }
                  
                if (round.CurrentMultiplier >= round.CrashPoint)
                {
                    await SendCrashEvent(round,table.TableId);
                    
                    _logger.LogInformation(
                        $"Round {round.RoundId} crashed at {round.CurrentMultiplier}",
                        round.RoundId,
                        round);
                    round.IsCrashed = true;

                    continue;
                }
                
                // INFO[Mahmoud] Bad Design 
                // if a tick is delayed, there will be lags the next tick sends the correct current multiplier, not just +0.1
                // PlayerMessageConsumer
                //     -> RoundEngine channel
                //     -> bet/cashout commands sequentially
                //
                // RoundsTicker
                //     -> reads active rounds
                //     -> calculates multiplier by elapsed time
                //     -> broadcasts tick directly to SignalR
                //     -> sends crash command when crash point reached
                //  OR 
                // Think later on a better design for round Memory shared state 
                round.CurrentMultiplier += 0.1m;

                await SendTickEvent(round,table.TableId);

                _logger.LogInformation(
                    $"Round {round.RoundId} tick {round.CurrentMultiplier}",
                    round.RoundId,
                    round.CurrentMultiplier,round);

            }
            await Task.Delay(TickInterval, stoppingToken);


        }
    }
}