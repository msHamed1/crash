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
    private readonly Channel<RoundCommand> _lifecycleChannel;
    private readonly Channel<RoundTickCommand> _tickChannel;
    private readonly IClientMessagePublisher _publisher;


 
    public RoundsService(ILogger<RoundsService> logger, GameEngineOptions options, IServiceScopeFactory scopeFactory,Rng.RngClient rngClient,IClientMessagePublisher publisher)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _options = options;
        _publisher = publisher;
        _rngClient = rngClient;
        _lifecycleChannel = Channel.CreateUnbounded<RoundCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        _tickChannel = Channel.CreateBounded<RoundTickCommand>(new BoundedChannelOptions(1024)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });
    }

    public ValueTask  EnqueueAsync(RoundCommand command, CancellationToken ct=default)
    {
        return command is RoundTickCommand tick
            ? _tickChannel.Writer.WriteAsync(tick, ct)
            : _lifecycleChannel.Writer.WriteAsync(command, ct);
        
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.WhenAll(
            ProcessChannelAsync(_lifecycleChannel.Reader, stoppingToken),
            ProcessChannelAsync(_tickChannel.Reader, stoppingToken));
    }

    private async Task ProcessChannelAsync<TCommand>(ChannelReader<TCommand> reader, CancellationToken ct)
        where TCommand : RoundCommand
    {
        await foreach (var message in reader.ReadAllAsync(ct))
        {
            await ProcessMessage(message, ct);
        }
    }

   private async Task ProcessMessage(RoundCommand command, CancellationToken ct)
    {

        try
        {
            if (command is RoundTickCommand tick)
            {
                _logger.LogDebug(
                    "Processing tick {TickSequence} for round {RoundId}, table {TableId}",
                    tick.TickSequence,
                    tick.RoundId,
                    tick.TableId);
            }
            else
            {
                _logger.LogInformation(
                    "Processing {MessageType} for table {TableId}",
                    command.MessageType,
                    command.TableId);
            }
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
        if (!IsCurrentRound(table, roundCrashCommand.RoundId))
            return;

        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IRoundRepository>();
        var roundId = long.Parse(roundCrashCommand.RoundId);
        var tableId = long.Parse(roundCrashCommand.TableId);
        var persisted = await repository.MarkCrashedAsync(roundId, tableId, table.FencingToken, ct);
        if (!persisted)
        {
            throw new InvalidOperationException(
                $"Could not mark round {roundId} as crashed because its fencing token or state is stale.");
        }

        await SendRoundCrashedCommand(roundCrashCommand, tableId, ct);
        // A new round may only be created after the previous crash is fenced, persisted,
        // and published. Otherwise a failed crash transition can be silently skipped.
        await Task.Delay(5000,ct);// Add Delay of 5 sec. 
        await EnqueueAsync(new NewRoundCommand { TableId = tableId.ToString() }, ct);
    }
    
    private async Task SendRoundCrashedCommand(RoundCrashCommand command, long tableId, CancellationToken ct)
    {
        _logger.LogInformation("Sending Round Crashed {RoundId} for Table {TableId}", command.RoundId, tableId);

        var message = new RoundCrashed
        {
            RoundId = long.Parse(command.RoundId),
            CurrentMultiplier = command.CurrentMultiplier,
            TableId = tableId,
            IsCrashed = true,
            TickSequence = command.TickSequence,
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
        if (table.GetCurrentRoundSnapshot() is null)
        {
            await CreateAndPublishRound(table.TableId, ct);
            return;
        }

        await SendNewRoundCreated(table, ct);
        
}
    
    private async Task ProcessNewRoundCommand(NewRoundCommand newRoundCommand, TableRuntimeState table,
        CancellationToken ct)
    {
        await CreateAndPublishRound(table.TableId, ct);
        
    }
    
    private async Task ProcessRoundTickCommand(RoundTickCommand command, TableRuntimeState table,
        CancellationToken ct)
    {
        if (!IsCurrentRound(table, command.RoundId))
            return;

        if (command.TickSequence == 1)
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IRoundRepository>();
            await repository.MarkRunningAsync(
                long.Parse(command.RoundId),
                long.Parse(command.TableId),
                table.FencingToken,
                ct);
        }

        await SendRoundTickCommand(command, long.Parse(command.TableId), ct);
    }

    private async Task  SendNewRoundCreated(TableRuntimeState table, CancellationToken ct)
    {
       
        var round = table.GetCurrentRoundSnapshot();
        if (round is null)
            return;

        _logger.LogInformation("Sending Round {RoundId} for Table {TableId}", round.RoundId, table.TableId);

        var message = new NewRoundInfo
        {
            RoundId = round.RoundId,
            CurrentMultiplier = round.CurrentMultiplier,
            StartsAt = round.StartsAt,
            IsCrashed = round.IsCrashed,
            TableId = table.TableId,
            MessageId = Guid.NewGuid().ToString(),

        };

        await _publisher.PublishAsync(message,ct);



    }
    
    
    
    private async Task SendRoundTickCommand(RoundTickCommand command, long tableId, CancellationToken ct)
    {
        var message = new RoundTick
        {
            RoundId = long.Parse(command.RoundId),
            CurrentMultiplier = command.CurrentMultiplier,
            TableId = tableId,
            TickSequence = command.TickSequence,
            MessageId = Guid.NewGuid().ToString(),

        };

        await _publisher.PublishAsync(message,ct);



    }
   

    private async Task<Round?> CreateAndPublishRound(long tableId,CancellationToken ct)
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
        StartsAt = round.StartTime,
        
    });
    var currentRound = table.GetCurrentRoundSnapshot();
    _logger.LogInformation("Current Round is",currentRound);

    await SendNewRoundCreated(table, ct);
     return updatedRound;


    }

    private static bool IsCurrentRound(TableRuntimeState table, string commandRoundId)
    {
        return long.TryParse(commandRoundId, out var roundId)
               && table.GetCurrentRoundSnapshot()?.RoundId == roundId;
    }
}
