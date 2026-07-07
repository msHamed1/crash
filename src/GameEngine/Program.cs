using Crash.Domain.Options;
using Crash.Rng;
using Crash.Persistence;
using Crash.Persistence.Migrations;
using GameEngine.Messaging;
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
builder.Services.AddGrpcClient<Rng.RngClient>(options =>
{
    options.Address = new Uri(rngAddress);
});
builder.Services.AddSingleton(dbWorkerBrokerOptions);

builder.Services.AddSingleton(playerBrokerOptions);
builder.Services.AddSingleton(gameEngineOptions);
builder.Services.AddSingleton<IDbWorkerMessagePublisher, DbWorkerMessagePublisher>();
builder.Services.AddSingleton<RoundEngine>();
builder.Services.AddHostedService<PlayerMessageConsumer>();
builder.Services.AddHostedService<Core>();

// Seeders 
builder.Services.AddScoped<IDatabaseSeeder, TablesSeeder>();

// Repositories
builder.Services.AddScoped<ITableRepository, TableRepository>();
builder.Services.AddScoped<IRoundRepository, RoundRepository>(); 
builder.Services.AddScoped<IOwnerRepository, OwnerRepository>();
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

builder.Logging.AddProvider(
    new DatabaseLoggerProvider(
        builder.Services.BuildServiceProvider()
            .GetRequiredService<IServiceScopeFactory>()
    )
);

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<DataContext>();
    await db.Database.MigrateAsync();

    var seeder = scope.ServiceProvider.GetRequiredService<IDatabaseSeeder>();
    await seeder.SeedAsync(CancellationToken.None);
}

app.MapGet("/", () => "GameEngine is running. POST /rounds/start to generate crash round entropy.");

app.MapPost("/rounds/start", async (
    StartRoundRequest? request,
    RoundEngine roundEngine,
    CancellationToken cancellationToken) =>
{
    var round = await roundEngine.StartRoundAsync(request ?? new StartRoundRequest(null, null, null, null), cancellationToken);
    return Results.Created($"/rounds/{round.RoundId}", round);
});

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
