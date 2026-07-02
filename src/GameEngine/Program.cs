using Crash.Rng;
using GameEngine.Messaging;
using GameEngine.Options;
using GameEngine.Services;

var builder = WebApplication.CreateBuilder(args);

var rngAddress = builder.Configuration["Services:Rng:Address"]
    ?? throw new InvalidOperationException("Services:Rng:Address is required.");
var brokerOptions = builder.Configuration
    .GetSection(BrokerOptions.SectionName)
    .Get<BrokerOptions>() ?? new BrokerOptions();
var gameEngineOptions = builder.Configuration
    .GetSection(GameEngineOptions.SectionName)
    .Get<GameEngineOptions>() ?? new GameEngineOptions();

builder.Services.AddGrpcClient<Rng.RngClient>(options =>
{
    options.Address = new Uri(rngAddress);
});
builder.Services.AddSingleton(brokerOptions);
builder.Services.AddSingleton(gameEngineOptions);
builder.Services.AddSingleton<RoundEngine>();
builder.Services.AddHostedService<PlayerMessageConsumer>();

var app = builder.Build();

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
