 using System.Threading.Channels;
 using Crash.Domain.Contracts.Commands;
 using Crash.Domain.Options;
 using Crash.Rng;
 
 namespace GameEngine.Services;

public sealed class RoundsService : BackgroundService
{
    public readonly Rng.RngClient _rngClient;
    public readonly ILogger<RoundsService> _logger;
    private readonly GameEngineOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly Channel<RoundCommand> _channel;

    public RoundsService(ILogger<RoundsService> logger, GameEngineOptions options, IServiceScopeFactory scopeFactory,Rng.RngClient rngClient)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _options = options;
        _rngClient = rngClient;
        _channel = Channel.CreateUnbounded<RoundCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    public ValueTask  EnqueueAsync(RoundCommand command, CancellationToken ct=default)
    {
        return _channel.Writer.WriteAsync(command, ct);
        
    }
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }
}