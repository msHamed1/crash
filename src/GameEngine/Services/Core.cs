using Crash.Domain.Entities;
using Crash.Domain.Options;
using GameEngine.Repository;

namespace GameEngine.Services;

public class Core:BackgroundService
{
    private static readonly TimeSpan OwnershipRefreshInterval = TimeSpan.FromSeconds(15);
    
    private readonly ILogger<Core> _logger;
    private readonly GameEngineOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;

    public Core(ILogger<Core> logger, GameEngineOptions options, IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _options = options;
    }
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var owner = await ValidateOwner(ct);

        if (owner is null)
        {
            _logger.LogInformation("No owner found for engine {EngineId}.", _options.EngineId);
            return;
        }

        _logger.LogInformation(
            "Game engine {EngineId} registered as owner {OwnerId}.",
            _options.EngineId,
            owner.Id);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RenewTables(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Error while refreshing table ownership.");
            }

            await Task.Delay(OwnershipRefreshInterval, ct);
        }
    }

    private async Task RenewTables(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();

        var tableRepository = scope.ServiceProvider
            .GetRequiredService<ITableRepository>();

        foreach (var table in _options.tokensPerTable)
        {
            try
            {
                var tableEntity = await tableRepository.RenewOwnership(
                    table.Key,
                    _options.OwnerId,
                    table.Value,
                    ct);

                if (tableEntity is null)
                {
                    // Renewal failed, so this engine must stop treating the table as owned.
                    // PlayerMessageConsumer watches this shared dictionary and cancels the
                    // RabbitMQ consumer for the table on its next reconciliation tick.
                    _options.tokensPerTable.TryRemove(table.Key, out _);
                    _logger.LogWarning(
                        "Game engine {EngineId} lost ownership for table {TableId}.",
                        _options.EngineId,
                        table.Key);
                    continue;
                }

                // Always overwrite the local fencing token with the DB-confirmed value.
                // If another engine won the lease race, RenewOwnership returns null instead.
                _options.tokensPerTable[tableEntity.Id] = tableEntity.FencingToken;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error renewing ownership for table {TableId}", table.Key);
            }
        }

        var newTable = await tableRepository.TryAcquireNewOwnership(_options.OwnerId, ct);

        if (newTable is not null)
        {
            // Adding the table here is enough for PlayerMessageConsumer to start consuming
            // the durable table queue without restarting this process.
            _options.tokensPerTable[newTable.Id] = newTable.FencingToken;
            _logger.LogInformation(
                "Game engine {EngineId} acquired ownership for table {TableId} with fencing token {FencingToken}.",
                _options.EngineId,
                newTable.Id,
                newTable.FencingToken);

            // Also update Redis routing here so gateways forward table events to this engine.
        }
    }
    private async Task<Owner?> ValidateOwner( CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();

        var owneRepository = scope.ServiceProvider
            .GetRequiredService<IOwnerRepository>();
            
        var instance =await owneRepository.GetByName(_options.EngineId,ct);
        if (instance is null)
        {
            Console.WriteLine(
                "No owner found for engine {0}",
                _options.EngineId
            );
           var newOwner = await owneRepository.CreateOwner( 
           new Owner {
                Name = _options.EngineId,
                Tables = new List<Table>()
            } ,ct);
           _options.OwnerId = newOwner.Id;

           return newOwner;
        }

        _options.OwnerId = instance.Id;

        return instance;
    }
}
