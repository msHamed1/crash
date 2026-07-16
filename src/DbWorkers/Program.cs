using Crash.Domain.Options;
using Crash.Persistence;
using DbWorkers.Consumers;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
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
var dbWorkerBrokerOptions= builder.Configuration.  
GetSection(DbBrokerOptions.SectionName)
    .Get<DbBrokerOptions>() ?? new DbBrokerOptions();
builder.Services.AddSingleton(dbWorkerBrokerOptions);

builder.Services.AddScoped<IDbWorkerMessageProcessor, DbWorkerMessageProcessor>();
builder.Services.AddHostedService<DbMessageConsumer>();

var app = builder.Build();

app.MapGet("/", () => "DbWorker is running.");


app.Run();
