using RngService.Data;
using RngService.Options;
using RngService.Services;

var builder = WebApplication.CreateBuilder(args);

var mysqlOptions = builder.Configuration
    .GetSection(MySqlOptions.SectionName)
    .Get<MySqlOptions>() ?? new MySqlOptions();
var rngOptions = builder.Configuration
    .GetSection(RngOptions.SectionName)
    .Get<RngOptions>() ?? new RngOptions();

builder.Services.AddGrpc();
builder.Services.AddSingleton(mysqlOptions);
builder.Services.AddSingleton(rngOptions);
builder.Services.AddSingleton<IRngRepository, RngRepository>();

var app = builder.Build();

await InitializeRepositoryAsync(app.Services, app.Logger, CancellationToken.None);

app.MapGrpcService<RngGrpcService>();
app.MapGet("/", () => "RngService is running. Use gRPC crash.rng.Rng/GenerateRoundEntropy.");

app.Run();

static async Task InitializeRepositoryAsync(
    IServiceProvider services,
    ILogger logger,
    CancellationToken cancellationToken)
{
    const int maxAttempts = 12;
    var delay = TimeSpan.FromSeconds(5);

    using var scope = services.CreateScope();
    var repository = scope.ServiceProvider.GetRequiredService<IRngRepository>();

    for (var attempt = 1; attempt <= maxAttempts; attempt++)
    {
        try
        {
            await repository.InitializeAsync(cancellationToken);
            return;
        }
        catch (Exception exception) when (attempt < maxAttempts)
        {
            // MySQL can be reachable after the container is marked started; retry before failing the service.
            logger.LogWarning(
                exception,
                "RNG database initialization failed on attempt {Attempt}/{MaxAttempts}. Retrying in {DelaySeconds}s.",
                attempt,
                maxAttempts,
                delay.TotalSeconds);

            await Task.Delay(delay, cancellationToken);
        }
    }

    await repository.InitializeAsync(cancellationToken);
}
