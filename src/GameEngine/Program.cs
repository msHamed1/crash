using Crash.Rng;
using GameEngine.Services;

var builder = WebApplication.CreateBuilder(args);

var rngAddress = builder.Configuration["Services:Rng:Address"]
    ?? throw new InvalidOperationException("Services:Rng:Address is required.");

builder.Services.AddGrpcClient<Rng.RngClient>(options =>
{
    options.Address = new Uri(rngAddress);
});
builder.Services.AddSingleton<RoundEngine>();

var app = builder.Build();

app.MapGet("/", () => "GameEngine is running. POST /rounds/start to generate crash round entropy.");

app.MapPost("/rounds/start", async (
    StartRoundRequest request,
    RoundEngine roundEngine,
    CancellationToken cancellationToken) =>
{
    var round = await roundEngine.StartRoundAsync(request, cancellationToken);
    return Results.Created($"/rounds/{round.RoundId}", round);
});

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
