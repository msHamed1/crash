using System.Text.Json;
using System.Text.Json.Serialization;
using Crash.Domain.Options;
using RealtimeGateway.Dev;
using RealtimeGateway.Hubs;
using RealtimeGateway.Messaging;
 
var builder = WebApplication.CreateBuilder(args);

var brokerOptions = builder.Configuration
    .GetSection(PlayerBrokerOptions.SectionName)
    .Get<PlayerBrokerOptions>() ?? new PlayerBrokerOptions();
var jwtOptions = builder.Configuration
    .GetSection(JwtOptions.SectionName)
    .Get<JwtOptions>() ?? new JwtOptions();

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
builder.Services.AddSingleton(brokerOptions);
builder.Services.AddSingleton(jwtOptions);
builder.Services.AddSingleton<IJwtConnectionValidator, JwtConnectionValidator>();
builder.Services.AddSingleton<IPlayerMessagePublisher, PlayerMessagePublisher>();

var app = builder.Build();

app.UseCors();

app.MapGet("/", () => "RealtimeGateway is running. Connect to /hubs/player with a JWT.");
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapPost("/dev/connections", (DevConnectionRequest request, HttpContext httpContext) =>
{
    var tableId = string.IsNullOrWhiteSpace(request.TableId) ? "table-1" : request.TableId.Trim();
    var username = string.IsNullOrWhiteSpace(request.Username) ? Guid.NewGuid().ToString("N") : request.Username.Trim();
    var name = string.IsNullOrWhiteSpace(request.Name) ? username : request.Name.Trim();
    var accessToken = DevJwtTokenFactory.Create(jwtOptions.SigningKey, tableId, username, name);
    var hubUrl = $"{httpContext.Request.Scheme}://{httpContext.Request.Host}/hubs/player";

    return Results.Ok(new DevConnectionResponse(hubUrl, accessToken, tableId, username, name));
});
app.MapHub<PlayerHub>("/hubs/player");

app.Run();
