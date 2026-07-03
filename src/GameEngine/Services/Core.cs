namespace GameEngine.Services;

public class Core:BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // it is not your Job To Renew Fencing Token Your Owner Ship is just to make sure All Events are processed in Memory or Sent to a durable Queue. 
        
        throw new NotImplementedException();
    }
}