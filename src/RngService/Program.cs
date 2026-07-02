using RngService.Data;
using RngService.Options;
using RngService.Services;

var builder = WebApplication.CreateBuilder(args);

var mysqlOptions = builder.Configuration
    .GetSection(MySqlOptions.SectionName)
    .Get<MySqlOptions>() ?? new MySqlOptions();

builder.Services.AddGrpc();
builder.Services.AddSingleton(mysqlOptions);
builder.Services.AddSingleton<IRngRepository, RngRepository>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var repository = scope.ServiceProvider.GetRequiredService<IRngRepository>();
    await repository.InitializeAsync(CancellationToken.None);
}

app.MapGrpcService<RngGrpcService>();
app.MapGet("/", () => "RngService is running. Use gRPC crash.rng.Rng/GenerateRoundEntropy.");

app.Run();
