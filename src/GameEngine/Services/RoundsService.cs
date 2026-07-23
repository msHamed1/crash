 using System.Collections.Concurrent;
 using System.Threading.Channels;
 using Crash.Contracts.Messaging.EngineToGateway.Rounds;
 using Crash.Domain.Entities;
 using Crash.Domain.Options;
 using Crash.Domain.State;
 using Crash.Persistence.Repositories;
 using Crash.Rng;
 using GameEngine.Messaging;
 using GameEngine.Repository;
 using GameEngine.Application.Commands;
 using GameEngine.Application.Commands.Bets;
 using GameEngine.Application.Commands.Players;
 using GameEngine.Application.Commands.Rounds;
 using GameEngine.Messaging.Publishers;

 namespace GameEngine.Services;

public sealed class RoundsService 
    : BackgroundService
{
    private readonly ILogger<RoundsService> logger;
    private readonly GameEngineOptions options;
    private readonly IServiceScopeFactory scopeFactory;
    private readonly Rng.RngClient rngClient;
    private readonly IWssGatewayPublisher publisher;
    private readonly BettingService bettingService;

    public RoundsService(
        ILogger<RoundsService> logger,
        GameEngineOptions options,
        IServiceScopeFactory scopeFactory,
        Rng.RngClient rngClient,
        IWssGatewayPublisher publisher,
        BettingService bettingService)
    {
        this.logger = logger;
        this.options = options;
        this.scopeFactory = scopeFactory;
        this.rngClient = rngClient;
        this.publisher = publisher;
        this.bettingService = bettingService;
    }

    private async Task SettleLatestRound()
    {
        // TODO settle old rounds when the engine starts

    }
    private readonly Channel<GameCommand> _lifecycleChannel = Channel.CreateUnbounded<GameCommand>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false
    });
    private readonly Channel<AdvanceRoundCommand> _tickChannel = Channel.CreateBounded<AdvanceRoundCommand>(new BoundedChannelOptions(1024)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropOldest
    });


    public ValueTask EnqueueAsync(GameCommand command, CancellationToken ct = default)
    {
        return command is AdvanceRoundCommand tick
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
        where TCommand : GameCommand
    {
        await foreach (var message in reader.ReadAllAsync(ct))
        {
            await ProcessMessage(message, ct);
        }
    }

   private async Task ProcessMessage(GameCommand command, CancellationToken ct)
    {

        try
        {
            if (command is AdvanceRoundCommand tick)
            {
                logger.LogDebug(
                    "Processing tick {TickSequence} for round {RoundId}, table {TableId}",
                    tick.TickSequence,
                    tick.RoundId,
                    tick.TableId);
            }
            if (command is ProcessAutoCashoutsCommand _)
            {
             
            }
            else   
            {
                logger.LogInformation(
                    "Processing {MessageType} for table {TableId}",
                    command.MessageType,
                    command.TableId);
            }
            options.Tables.TryGetValue(long.Parse(command.TableId), out var table);
            if (table is null)
            {
                logger.LogWarning("Ignoring command {MessageType} for unowned table {TableId}", command.MessageType, command.TableId);
                return;
            }
            switch (command)
            {
                case AddPlayerToTableCommand playerJoinedCommand:
                    await ProcessAddPlayerToTableCommand(
                        playerJoinedCommand,
                        table,
                        ct);
                    break;
                case AdvanceRoundCommand roundTickCommand:
                    await ProcessAdvanceRoundCommand(
                        roundTickCommand,
                        table,
                        ct);
                    break;
                case PlaceBetCommand playerBetCommand:
                    await ProcessPlaceBetCommand(
                        playerBetCommand,
                        table,
                        ct);
                    break;
                case CashOutBetCommand cashOutCommand:
                    await bettingService.CashOutAsync(cashOutCommand, table, ct);
                    break;
                case CrashRoundCommand roundCrashCommand:
                    await ProcessRoundCrashedCommand(
                        roundCrashCommand,
                        table,
                        ct);
                    break;
                case ProcessAutoCashoutsCommand autoCashoutsCommand:
                    await  bettingService.ProcessAutoCashoutsAsync(
                        table,
                        long.Parse(autoCashoutsCommand.RoundId),
                        autoCashoutsCommand.CurrentMultiplier,
                        ct);
                    break;
                case CreateRoundCommand newRoundCommand:
                    await ProcessCreateRoundCommand(
                        newRoundCommand,
                        table,
                        ct);
                    break;
            }
        }
        catch (Exception e)
        {
            logger.LogError(e, "Error processing Table {TableId}", command.TableId);
            
        }
    
        
    }
    private async Task ProcessRoundCrashedCommand(CrashRoundCommand roundCrashCommand, TableRuntimeState table,
        CancellationToken ct)
    {
        if (!IsCurrentRound(table, roundCrashCommand.RoundId))
            return;

        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IRoundRepository>();
        var roundId = long.Parse(roundCrashCommand.RoundId);
        var tableId = long.Parse(roundCrashCommand.TableId);
        var persisted = await repository.MarkCrashedAsync(roundId, tableId, table.FencingToken, ct);
        if (!persisted)
        {
            throw new InvalidOperationException(
                $"Could not mark round {roundId} as crashed because its fencing token or state is stale.");
        }
        // Database now confirms that this engine owns the crashed round.
        await bettingService.SettleCrashedRoundAsync(
            table,
            roundId,
            ct);

        await SendRoundCrashedCommand(roundCrashCommand, tableId, ct);
        // A new round may only be created after the previous crash is fenced, persisted,
        // and published. Otherwise a failed crash transition can be silently skipped.
        await EnqueueAsync(new CreateRoundCommand { TableId = tableId.ToString() }, ct);
    }
    
    private async Task SendRoundCrashedCommand(CrashRoundCommand command, long tableId, CancellationToken ct)
    {
        logger.LogInformation("Sending Round Crashed {RoundId} for Table {TableId}", command.RoundId, tableId);

        var message = new RoundCrashed
        {
            RoundId = long.Parse(command.RoundId),
            CurrentMultiplier = command.CurrentMultiplier,
            TableId = tableId,
            IsCrashed = true,
            TickSequence = command.TickSequence,
            MessageId = Guid.NewGuid().ToString(),
        };

        await publisher.PublishAsync(message,ct);
    }
    private async Task ProcessAddPlayerToTableCommand(AddPlayerToTableCommand command, TableRuntimeState table,
        CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var playerRepository = scope.ServiceProvider.GetRequiredService<IPlayerRepository>();
        var player = await playerRepository.GetById(long.Parse(command.PlayerId), ct);
        if (player is null)
        {
            logger.LogError("Player {PlayerId} does not exist", command.PlayerId);
            return;
        }

        table.AddPlayer(new PlayerRuntimeState
        {
            PlayerId = player.Id,
            Balance = player.BalanceInUSD,
            ExternalId =  player.ExternalId,
            
        });
        if (table.GetCurrentRoundSnapshot() is null)
        {
            await CreateAndPublishRound(table.TableId, ct);
            return;
        }

        await SendNewRoundCreated(table, ct);
        
}
    
    private async Task ProcessCreateRoundCommand(CreateRoundCommand command, TableRuntimeState table,
        CancellationToken ct)
    {
        await CreateAndPublishRound(table.TableId, ct);
        
    }
    
    private async Task ProcessAdvanceRoundCommand(AdvanceRoundCommand command, TableRuntimeState table,
        CancellationToken ct)
    {
        if (!IsCurrentRound(table, command.RoundId))
            return;

        if (command.TickSequence == 1)
        {
            using var scope = scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IRoundRepository>();
            await repository.MarkRunningAsync(
                long.Parse(command.RoundId),
                long.Parse(command.TableId),
                table.FencingToken,
                ct);
        }

        await SendRoundUpdated(command, long.Parse(command.TableId), ct);
    }

    private async Task ProcessPlaceBetCommand(PlaceBetCommand command, TableRuntimeState table, CancellationToken ct)
    {
        await bettingService.PlaceBetAsync(command, table, ct);
    }
    
    
    private async Task  SendNewRoundCreated(TableRuntimeState table, CancellationToken ct)
    {
       
        var round = table.GetCurrentRoundSnapshot();
        if (round is null)
            return;

        logger.LogInformation("Sending Round {RoundId} for Table {TableId}", round.RoundId, table.TableId);

        var message = new RoundCreated
        {
            RoundId = round.RoundId,
            CurrentMultiplier = round.CurrentMultiplier,
            StartsAt = round.StartsAt,
            IsCrashed = round.IsCrashed,
            TableId = table.TableId,
            MessageId = Guid.NewGuid().ToString(),

        };

        await publisher.PublishAsync(message,ct);



    }
    
    
    
    private async Task SendRoundUpdated(AdvanceRoundCommand command, long tableId, CancellationToken ct)
    {
        var message = new RoundUpdated
        {
            RoundId = long.Parse(command.RoundId),
            CurrentMultiplier = command.CurrentMultiplier,
            TableId = tableId,
            TickSequence = command.TickSequence,
            MessageId = Guid.NewGuid().ToString(),

        };

        await publisher.PublishAsync(message,ct);



    }
    
    
   
   

    private async Task<Round?> CreateAndPublishRound(long tableId,CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repo= scope.ServiceProvider.GetRequiredService<IRoundRepository>();

        
      var fToken =  options.Tables.TryGetValue(tableId, out var table );

      if (!fToken) return null;
      if(table is null)return null;    

     var round= await repo.CreateRoundAsync(tableId, table.FencingToken, ct);

    var rngEntropy= await rngClient.GenerateRoundEntropyAsync(new GenerateRoundEntropyRequest
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

    var updatedRound= await repo.UpdateRoundEntropyAsync(round.Id,(decimal) rngEntropy.CrashPoint, rngEntropy.RngId, ct);
     
    table.SetCurrentRound(new RoundRuntimeState
    {
        RoundId = round.Id,
        CrashPoint = (decimal)rngEntropy.CrashPoint,
        StartsAt = round.StartTime,
        
    });
    await SendNewRoundCreated(table, ct);
     return updatedRound;


    }

    private static bool IsCurrentRound(TableRuntimeState table, string commandRoundId)
    {
        return long.TryParse(commandRoundId, out var roundId)
               && table.GetCurrentRoundSnapshot()?.RoundId == roundId;
    }
}
