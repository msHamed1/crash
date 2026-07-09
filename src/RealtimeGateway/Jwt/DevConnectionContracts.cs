namespace RealtimeGateway.Jwt;

 
public sealed record DevConnectionResponse(
    string HubUrl,
    string AccessToken,
    string TableId,
    string Username,
    string Name);
