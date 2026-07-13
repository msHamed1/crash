 using System.Collections.Concurrent;
 using System.Threading.Channels;
 using Crash.Domain.Contracts.Commands;
 using Crash.Domain.Contracts.ServerMessages;
 using Crash.Domain.Entities;
 using Crash.Domain.Options;
 using Crash.Domain.State;
 using Crash.Persistence.Repositories;
 using Crash.Rng;
 using GameEngine.Messaging;
 using GameEngine.Repository;

 namespace GameEngine.Services;

public sealed class RoundsService : BackgroundService
{
    private readonly Rng.RngClient _rngClient;
    private readonly ILogger<RoundsService> _logger;
    private readonly GameEngineOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly Channel<RoundCommand> _channel;
    private readonly IClientMessagePublisher _publisher;


 
    public RoundsService(ILogger<RoundsService> logger, GameEngineOptions options, IServiceScopeFactory scopeFactory,Rng.RngClient rngClient,IClientMessagePublisher publisher)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _options = options;
        _publisher = publisher;
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
        while (!stoppingToken.IsCancellationRequested)
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

        try
        {
            _logger.LogInformation("Processing Table {TableId}", command.TableId,command );
            _options.Tables.TryGetValue(long.Parse(command.TableId), out var table);
            if (table is null)
            {
                _logger.LogWarning("Ignoring command {MessageType} for unowned table {TableId}", command.MessageType, command.TableId);
                return;
            }
            if (command is PlayerJoinedCommand playerJoinedCommand)
            {
                await ProcessPlayerJoinedCommand(
                    playerJoinedCommand,
                    table,
                    ct);
            }
            
            
            if (command is RoundTickCommand roundTickCommand)
            {
                await ProcessRoundTickCommand(
                    roundTickCommand,
                    table,
                    ct);
            }
            
             if (command is RoundCrashCommand roundCrashCommand)
            {
                await ProcessRoundCrashedCommand(
                    roundCrashCommand,
                    table,
                    ct);
            }
            if (command is NewRoundCommand newRoundCommand)
            {
                await ProcessNewRoundCommand(
                    newRoundCommand,
                    table,
                    ct);
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error processing Table {TableId}", command.TableId);
            
        }
    
        
    }
    private async Task ProcessRoundCrashedCommand(RoundCrashCommand roundCrashCommand, TableRuntimeState table,
        CancellationToken ct)
    {
        
        SendRoundCrashedCommand(table, ct);
        return;
        
    }
    
    private async Task  SendRoundCrashedCommand(TableRuntimeState table, CancellationToken ct)
    {
       
        _logger.LogInformation("Sending Round Crashed {RoundId} for Table {TableId}",table.CurrentRound.RoundId,table);

        var message = new RoundCrashed
        {
            RoundId = table.CurrentRound.RoundId,
            CurrentMultiplier = table.CurrentRound.CurrentMultiplier,
            TableId = table.TableId,
            IsCrashed = table.CurrentRound.IsCrashed,
            MessageId = Guid.NewGuid().ToString(),
            
        };

        await _publisher.PublishAsync(message,ct);



    }
    private async Task ProcessPlayerJoinedCommand(PlayerJoinedCommand command, TableRuntimeState table,
        CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var playerRepository = scope.ServiceProvider.GetRequiredService<IPlayerRepository>();
        var player = await playerRepository.GetById(long.Parse(command.PlayerId), ct);
        if (player is null)
        {
            _logger.LogError("Player {PlayerId} does not exist", command.PlayerId);
            return;
        }

        table.AddPlayer(new PlayerRuntimeState
        {
            PlayerId = player.Id,
            Balance = player.BalanceInUSD
        });
        if (table.CurrentRound is null)
        {
         await CreateRound(table.TableId,   ct);
         return;
        }

        SendNewRoundCreated(table, ct);
        return;
        
}
    
    private async Task ProcessNewRoundCommand(NewRoundCommand newRoundCommand, TableRuntimeState table,
        CancellationToken ct)
    {
        await CreateRound(table.TableId,   ct);

        SendNewRoundCreated(table, ct);
        return;
        
    }
    
    private async Task ProcessRoundTickCommand(RoundTickCommand command, TableRuntimeState table,
        CancellationToken ct)
    {
        
        SendRoundTickCommand(table, ct);
        return;
        
    }

    private async Task  SendNewRoundCreated(TableRuntimeState table, CancellationToken ct)
    {
       
            _logger.LogInformation("Sending Round {RoundId} for Table {TableId}",table.CurrentRound.RoundId,table);

            var message = new NewRoundInfo
            {
               RoundId = table.CurrentRound.RoundId,
               CurrentMultiplier = table.CurrentRound.CurrentMultiplier,
               StartsAt = table.CurrentRound.StartsAt,
               IsCrashed = table.CurrentRound.IsCrashed,
               TableId = table.TableId,
               MessageId = Guid.NewGuid().ToString(),

            };

        await _publisher.PublishAsync(message,ct);



    }
    
    
    
    private async Task  SendRoundTickCommand(TableRuntimeState table, CancellationToken ct)
    {
       
        _logger.LogInformation("Sending Round {RoundId} for Table {TableId}",table.CurrentRound.RoundId,table);

        var message = new RoundTick
        {
            RoundId = table.CurrentRound.RoundId,
            CurrentMultiplier = table.CurrentRound.CurrentMultiplier,
            TableId = table.TableId,
            MessageId = Guid.NewGuid().ToString(),

        };

        await _publisher.PublishAsync(message,ct);



    }
   

    private async Task<Round?> CreateRound(long tableId,CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var repo= scope.ServiceProvider.GetRequiredService<IRoundRepository>();

        
      var fToken =  _options.Tables.TryGetValue(tableId, out var table );

      if (!fToken) return null;
      if(table is null)return null;    

     var round= await repo.CreateRoundAsync(tableId, table.FencingToken, ct);

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
         return null;
     };
     Console.WriteLine(rngEntropy);

    var updatedRound= await repo.UpdateRoundEntropyAsync(round.Id,(decimal) rngEntropy.CrashPoint, rngEntropy.RngId, ct);
     
    table.SetCurrentRound(new RoundRuntimeState
    {
        RoundId = round.Id,
        CrashPoint = (decimal)rngEntropy.CrashPoint,
        CurrentMultiplier = 1.1m,
        StartsAt = round.StartTime,
        IsCrashed = false,
        


    });

    SendNewRoundCreated(table, ct);
     return updatedRound;


    }
}