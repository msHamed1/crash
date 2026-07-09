 using System.Collections.Concurrent;
 using System.Threading.Channels;
 using Crash.Domain.Contracts.Commands;
 using Crash.Domain.Entities;
 using Crash.Domain.Options;
 using Crash.Rng;
 using GameEngine.Repository;

 namespace GameEngine.Services;

public sealed class RoundsService : BackgroundService
{
    public readonly Rng.RngClient _rngClient;
    public readonly ILogger<RoundsService> _logger;
    private readonly GameEngineOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly Channel<RoundCommand> _channel;

 
    public RoundsService(ILogger<RoundsService> logger, GameEngineOptions options, IServiceScopeFactory scopeFactory,Rng.RngClient rngClient)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _options = options;
        _rngClient = rngClient;
        _channel = Channel.CreateUnbounded<RoundCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    public ValueTask  EnqueueAsync(RoundCommand command, CancellationToken ct=default)
    {
        return _channel.Writer.WriteAsync(command, ct);
        
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (stoppingToken.IsCancellationRequested == false)
        {
            await foreach (var message in _channel.Reader.ReadAllAsync(stoppingToken))
            {
                await ProcessMessage(message, stoppingToken);
            }
        }

        return;
    }

   private async Task ProcessMessage(RoundCommand command, CancellationToken ct)
    {
        if (command.MessageType == "Joined")
        {
            _logger.LogInformation("A Player Joined the Table hehe");
            Console.WriteLine("A Player Joined the Table hehe");
        }
        
    }

    private async Task<Round?> CreateRound(long tableId,CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var repo= scope.ServiceProvider.GetRequiredService<IRoundRepository>();

        
      var fToken =  _options.Tables.TryGetValue(tableId, out var token );

      if (!fToken) return null;
      if(token is null)return null;    

     var round= await repo.CreateRoundAsync(tableId, token.FencingToken, ct);

    var rngEntropy= await _rngClient.GenerateRoundEntropyAsync(new GenerateRoundEntropyRequest
     {
         ClientSeed = "client-seed-todo",
         Nonce = round.Nonce,
         RoundId = round.Id.ToString(),
         TableId = tableId.ToString()
     });

     if (rngEntropy is null)
     {
         //  sleep try again .
     };

    var updatedRound= await repo.UpdateRoundEntropyAsync(tableId,(decimal) rngEntropy.CrashPoint, rngEntropy.RngId, ct);
     
    
     return updatedRound;


    }
}