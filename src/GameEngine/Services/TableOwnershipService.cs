using Crash.Domain.Entities;
using Crash.Domain.Options;
using Crash.Domain.State;
using GameEngine.Repository;

namespace GameEngine.Services;

public class TableOwnershipService(ILogger<TableOwnershipService> logger, GameEngineOptions options, IServiceScopeFactory scopeFactory)
    : BackgroundService
{
    private static readonly TimeSpan OwnershipRefreshInterval = TimeSpan.FromSeconds(15);


    private async Task<Owner> ValidateOwner(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var ownerRepository = scope.ServiceProvider.GetRequiredService<IOwnerRepository>();
        var instance = await ownerRepository.GetByName(options.EngineId, ct);
        if (instance is null)
        {
            logger.LogError(
                "No owner found for engine {EngineId}. Creating a new owner.",
                options.EngineId);
            var newOwner = await ownerRepository.CreateOwner( 
                new Owner {
                    Name = options.EngineId,
                    Tables = new List<Table>()
                } ,ct);
            options.OwnerId = newOwner.Id;
            logger.LogInformation(
                "New Game engine {EngineId} Registered as owner {OwnerId}.",
                options.EngineId,
                options.OwnerId);
            return newOwner;
            
        }
        
        options.OwnerId = instance.Id;
      
        return instance;

    }

    private async Task RenewTables(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var tableRepository = scope.ServiceProvider.GetRequiredService<ITableRepository>();

        try
        {
            
            // First Make sure Our Current Tables are renewed 
            foreach (var keyValuePair in options.Tables)

            {

                var tableEntity = await tableRepository.RenewOwnership(
                    tableId: keyValuePair.Key,
                    ownerId: options.OwnerId,
                    fencingToken: keyValuePair.Value.FencingToken,
                    ct: ct
                );

                if (tableEntity is null)
                {
                    // Renewal failed, so this engine must stop treating the table as owned.
                    // Services  that implments RabbitMQ consumer for the table should cancel consuming on its next reconciliation tick.


                    // Store last Table/Round Snapshot in DB
                    // In case a new engine will pick up this 
                    // To be honest : this should be done on each update on the Table memory store. 
                    // we will do that later.
                    options.Tables.TryRemove(keyValuePair.Key, out _);
                    logger.LogWarning("Game engine {EngineId} lost ownership for table {TableId}.", options.EngineId,
                        keyValuePair.Key);
                    continue;

                }


                // Always overwrite the local fencing token with the DB-confirmed value.
                // If another engine won the lease race, RenewOwnership returns null instead.

                // BUG Do not distroy the current memory Table snapshot
                keyValuePair.Value.SetFencingToken(tableEntity.FencingToken);
            }
            
            // Try Acquire other tables .
            var newTable = await tableRepository.TryAcquireNewOwnership(options.OwnerId, ct);
            if (newTable is not null)
            {
                // Adding the table here is enough for PlayerMessageConsumer to start consuming
                // the durable table queue without restarting this process.
            
                // TODO  Recover the active round from the database or durable event stream.
                // If database renewal repeatedly throws, the engine keeps operating even after its lease may have expired. Track the locally confirmed LeaseExpiresAt and stop processing when it passes.
                // Only one table is acquired every 15 seconds. Consider acquiring repeatedly until capacity is reached.
                // Put Task.Delay inside the cancellation handling.
                options.Tables[newTable.Id] = new TableRuntimeState(newTable.Id);
                options.Tables[newTable.Id].SetFencingToken(newTable.FencingToken);
           
             
                logger.LogInformation(
                    "Game engine {EngineId} acquired ownership for table {TableId} with fencing token {FencingToken}.",
                    options.EngineId,
                    newTable.Id,
                    newTable.FencingToken);

                // Also update Redis routing here so gateways forward table events to this engine.
            }

        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occured while renewing tables.");
            throw;
        }
       
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
     

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // make sure the engine is registered.
                // We can do it once and store it in redis with long TTL.  
                await ValidateOwner(stoppingToken);
                
                await RenewTables(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                logger.LogError(e, "Error while refreshing table ownership.");


            }
            finally
            {
                await Task.Delay(OwnershipRefreshInterval, stoppingToken);

            }
            
            
        }

        

       
    }
}