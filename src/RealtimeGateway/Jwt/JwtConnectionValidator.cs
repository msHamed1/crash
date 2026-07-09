using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Crash.Domain.Options;
using Microsoft.AspNetCore.SignalR;
using Microsoft.IdentityModel.Tokens;

namespace RealtimeGateway.Jwt;

public interface IJwtConnectionValidator
{
    PlayerConnectionContext Validate(HubCallerContext context);
}

public sealed record PlayerConnectionContext(
    string PlayerId,
    string ExternalId,
    string Type,
    string TableId);

public sealed class JwtConnectionValidator(JwtOptions jwtOptions) : IJwtConnectionValidator
{
    public PlayerConnectionContext Validate(HubCallerContext context)
    {
        var token = GetToken(context);

        if (string.IsNullOrWhiteSpace(token))
        {
            throw new HubException("Access token is required.");
        }

        var tokenHandler = new JwtSecurityTokenHandler();
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtOptions.SigningKey));

        try
        {
            var principal = tokenHandler.ValidateToken(token, new TokenValidationParameters
            {
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = key,
                ValidateIssuer = false,
                ValidateAudience = false,
                ValidateLifetime = true,
                ClockSkew = TimeSpan.FromSeconds(30)
            }, out _);

            return new PlayerConnectionContext(
                GetRequiredClaim(principal, "playerId"),
                GetRequiredClaim(principal, "externalId"),
                GetRequiredClaim(principal, "type"),
                GetRequiredClaim(principal, "tableId"));
        }
        catch (SecurityTokenException ex)
        {
            throw new HubException("Invalid access token.", ex);
        }
        catch (ArgumentException ex)
        {
            throw new HubException("Invalid access token.", ex);
        }
    }

    private static string? GetToken(HubCallerContext context)
    {
        var httpContext = context.GetHttpContext();

        var queryToken = httpContext?.Request.Query["access_token"].FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(queryToken))
        {
            return queryToken;
        }

        var authorization = httpContext?.Request.Headers.Authorization.FirstOrDefault();
        if (authorization?.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) == true)
        {
            return authorization["Bearer ".Length..].Trim();
        }

        return null;
    }

    private static string GetRequiredClaim(ClaimsPrincipal principal, string claimType)
    {
        var value = principal.FindFirstValue(claimType);

        if (string.IsNullOrWhiteSpace(value))
        {
            throw new HubException($"Token is missing required claim '{claimType}'.");
        }

        return value;
    }
}
