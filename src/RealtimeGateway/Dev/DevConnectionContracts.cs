namespace RealtimeGateway.Dev;

public sealed record DevConnectionRequest(
    string? Name,
    string? Username,
    string? TableId);

public sealed record DevConnectionResponse(
    string HubUrl,
    string AccessToken,
    string TableId,
    string Username,
    string Name);
