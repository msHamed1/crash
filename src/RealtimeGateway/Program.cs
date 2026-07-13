using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Domain.Options;
using Crash.Persistence;
using Crash.Persistence.Logger;
using Crash.Persistence.Repositories;
using GameEngine.Repository;
using Microsoft.EntityFrameworkCore;
using RealtimeGateway.Hubs;
using RealtimeGateway.Jwt;
using RealtimeGateway.Messaging;
 
var builder = WebApplication.CreateBuilder(args);

var brokerOptions = builder.Configuration
    .GetSection(PlayerBrokerOptions.SectionName)
    .Get<PlayerBrokerOptions>() ?? new PlayerBrokerOptions();
var jwtOptions = builder.Configuration
    .GetSection(JwtOptions.SectionName)
    .Get<JwtOptions>() ?? new JwtOptions();
var fanoutOptions = builder.Configuration
    .GetSection(FanoutOptions.SectionName)
    .Get<FanoutOptions>() ?? new FanoutOptions();

builder.Services.AddSingleton(fanoutOptions);
if (string.IsNullOrWhiteSpace(jwtOptions.SigningKey))
{
    throw new InvalidOperationException("Jwt:SigningKey is required.");
}

builder.Services
    .AddCors(options =>
    {
        options.AddDefaultPolicy(policy =>
        {
            policy
                .SetIsOriginAllowed(_ => true)
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials();
        });
    });
builder.Services
    .AddSignalR()
    .AddJsonProtocol(options =>
    {
        options.PayloadSerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
    });
builder.Services.AddControllers();
builder.Services.AddSingleton(brokerOptions);
builder.Services.AddSingleton(jwtOptions);
builder.Services.AddSingleton<IJwtConnectionValidator, JwtConnectionValidator>();
builder.Services.AddSingleton<IPlayerMessagePublisher, PlayerMessagePublisher>();
builder.Services.AddScoped<IPlayerRepository, PlayerRepository>();
builder.Services.AddScoped<ITableRepository, TableRepository>();

// Hosted service ;
builder.Services.AddHostedService<ClientMessagesConsumer>();
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

builder.Logging.AddProvider(
    new DatabaseLoggerProvider(
        builder.Services.BuildServiceProvider()
            .GetRequiredService<IServiceScopeFactory>()
    )
);
 var app = builder.Build();
 
app.UseCors();

app.MapGet("/", () => "RealtimeGateway is running. Connect to /hubs/player with a JWT.");
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapControllers();
app.MapHub<PlayerHub>("/hubs/player");

app.Run();
