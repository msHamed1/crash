using Crash.Domain.Options;
using Crash.Rng;
using Crash.Persistence;
using Crash.Persistence.Logger;
using Crash.Persistence.Repositories;
using GameEngine.Messaging;
using GameEngine.Messaging.Consumers;
using GameEngine.Messaging.Publishers;
using GameEngine.Repository;
using GameEngine.Seeders;
using GameEngine.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

var rngAddress = builder.Configuration["Services:Rng:Address"]
    ?? throw new InvalidOperationException("Services:Rng:Address is required.");
var playerBrokerOptions = builder.Configuration
    .GetSection(PlayerBrokerOptions.SectionName)
    .Get<PlayerBrokerOptions>() ?? new PlayerBrokerOptions();
var gameEngineOptions = builder.Configuration
    .GetSection(GameEngineOptions.SectionName)
    .Get<GameEngineOptions>() ?? new GameEngineOptions();

var dbWorkerBrokerOptions = builder.Configuration
    .GetSection(DbBrokerOptions.SectionName)
    .Get<DbBrokerOptions>() ?? new DbBrokerOptions();

var fanoutOptions = builder.Configuration
    .GetSection(FanoutOptions.SectionName)
    .Get<FanoutOptions>() ?? new FanoutOptions();

builder.Services.AddSingleton(fanoutOptions);

builder.Services.AddGrpcClient<Rng.RngClient>(options =>
{
    options.Address = new Uri(rngAddress);
});
builder.Services.AddSingleton(dbWorkerBrokerOptions);

builder.Services.AddSingleton(playerBrokerOptions);
builder.Services.AddSingleton(gameEngineOptions);
builder.Services.AddSingleton<BettingService>();
builder.Services.AddSingleton<RoundsService>();
builder.Services.AddSingleton<IDbWorkerPublisher, DbWorkerPublisher>();
builder.Services.AddSingleton<IWssGatewayPublisher,WssGatewayPublisher>();
 builder.Services.AddHostedService<WssGatewayConsumer>();
builder.Services.AddHostedService<DbWorkerConsumer>();
builder.Services.AddHostedService<TableOwnershipService>();
builder.Services.AddSingleton<RoundsTicker>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RoundsTicker>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RoundsService>());

// Logs 
builder.Services.AddSingleton<DatabaseLogQueue>();
builder.Services.AddHostedService<DataBaseLogWriter>();
builder.Services.AddHostedService<DataBaseLogWriter>();

builder.Services.AddSingleton<ILoggerProvider, DatabaseLoggerProvider>();

// Seeders 
builder.Services.AddScoped<IDatabaseSeeder, TablesSeeder>();

// Repositories
builder.Services.AddScoped<ITableRepository, TableRepository>();
builder.Services.AddScoped<IRoundRepository, RoundRepository>(); 
builder.Services.AddScoped<IOwnerRepository, OwnerRepository>();
builder.Services.AddScoped<IPlayerRepository, PlayerRepository>();
builder.Services.AddScoped<IBetRepository, BetRepository>();

builder.Services.AddDbContext<DataContext>(options =>
{
    var connectionString = builder.Configuration.GetConnectionString("MySql")
        ?? builder.Configuration["MySql:ConnectionString"]
        ?? throw new InvalidOperationException("MySql connection string is required.");

    options.UseMySql(
        connectionString,
        ServerVersion.AutoDetect(connectionString)
    );
});
// Add DbLogger;
// builder.Logging.AddProvider(
//     new DatabaseLoggerProvider(
//         builder.Services.BuildServiceProvider()
//             .GetRequiredService<IServiceScopeFactory>()
//         )
//     );

// builder.Logging.AddProvider(
//     new DatabaseLoggerProvider(
//         builder.Services.BuildServiceProvider()
//             .GetRequiredService<IServiceScopeFactory>()
//     )
// );

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<DataContext>();
    await db.Database.MigrateAsync();

    var seeder = scope.ServiceProvider.GetRequiredService<IDatabaseSeeder>();
    await seeder.SeedAsync(CancellationToken.None);
}

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
