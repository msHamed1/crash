using Crash.Domain.Contracts.Commands;
using Crash.Domain.Options;

namespace GameEngine.Services;

public sealed class RoundTickMap
{
   public string RoundId  { get; init; } 
   public  string TableId  { get; init; } 
   public  decimal CurrentTick  { get; set; } 
   public decimal MaxTick  { get; init; } 
   public decimal MinTick  { get; init; } 
   public DateTimeOffset StartsAt { get; init; }
   
}
public sealed class RoundsTicker:BackgroundService
{
    
    private readonly ILogger<RoundsTicker> _logger;
    private Dictionary<string, RoundTickMap> _rounds = new();
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

    private async Task SendCrashEvent(RoundTickMap roundTickMap)
    {
        var envelop = new RoundCrashCommand
        {
            CurrentMultiplier = roundTickMap.CurrentTick,
            RoundId = roundTickMap.RoundId,
            TableId = roundTickMap.TableId,

        };
       await _roundsService.EnqueueAsync(envelop);

       var newRoundCommand = new NewRoundCommand()
       {
           TableId = roundTickMap.TableId,
       };
       await _roundsService.EnqueueAsync(newRoundCommand);
       
        
    }
    
    
    private async Task SendTickEvent(RoundTickMap roundTickMap)
    {
        var envelop = new RoundTickCommand 
        {
            CurrentMultiplier = roundTickMap.CurrentTick,

            RoundId = roundTickMap.RoundId,
            TableId = roundTickMap.TableId,

        };
        await _roundsService.EnqueueAsync(envelop);
       
        
    }

    public void AddNewRound(RoundTickMap roundTickMap)
    {
        lock (_lock)
        {
            _rounds[roundTickMap.RoundId] = roundTickMap;
        }
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            List<RoundTickMap> roundsSnapshot;

            lock (_lock)
            {
                roundsSnapshot = _rounds.Values.ToList();
            }

            foreach (var round in roundsSnapshot)
            {
                if (round.StartsAt > DateTimeOffset.UtcNow)
                    continue;

                if (round.CurrentTick >= round.MaxTick)
                {
                    await SendCrashEvent(round);

                    lock (_lock)
                    {
                        _rounds.Remove(round.RoundId);
                    }

                    _logger.LogInformation(
                        $"Round {round.RoundId} crashed at {round.CurrentTick}",
                        round.RoundId,
                        round.CurrentTick,round);

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
                round.CurrentTick += 0.1m;

                await SendTickEvent(round);

                _logger.LogInformation(
                    $"Round {round.RoundId} tick {round.CurrentTick}",
                    round.RoundId,
                    round.CurrentTick,round);
            }

            await Task.Delay(TickInterval, stoppingToken);
        }
    }
}