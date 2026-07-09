using Crash.Domain.Entities;

namespace RealtimeGateway.Authentications.Dto;

public class PlayerLoginRes
{
    public required string Token { get; set; }
    public required Player Player { get; set; }
    
    public required string HubUrl { get; set; }
    
}