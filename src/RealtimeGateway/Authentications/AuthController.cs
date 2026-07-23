using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Crash.Domain.Entities;
using Crash.Persistence.Repositories;
using GameEngine.Repository;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using RealtimeGateway.Authentications.Dto;

namespace RealtimeGateway.Authentications;

[ApiController]
[Route("api/players")]
public class AuthController: ControllerBase
{
    
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;

    public AuthController(IServiceScopeFactory scopeFactory, IConfiguration configuration)
    {
        _scopeFactory = scopeFactory;
        _configuration = configuration;
    }
    
    
    [HttpPost("login")]
    public async Task<ActionResult<PlayerLoginRes>> Login(
        [FromBody] PlayerLoginReq request,
        CancellationToken ct)
    {
        var username = request.Username.Trim();

        if (string.IsNullOrWhiteSpace(username))
        {
            return BadRequest("Username is required.");
        }

        using var  scope = _scopeFactory.CreateScope();
        var playerContext = scope.ServiceProvider.GetRequiredService<IPlayerRepository>();
        var player = await playerContext.GetPlayerByUsername(username, ct);

        if (player is null)
        {
            player =await playerContext.Create(username, ct);
           
        }
        
        // Find the available Table and add the user to that Table.
        var tableContext = scope.ServiceProvider.GetRequiredService<ITableRepository>();
        var table= await tableContext.GetOrCreateTableForPlayer(player, ct);

        var token = GenerateJwt(player,table);
         var hubUrl = $"{Request.Scheme}://{Request.Host}/hubs/player";

        return Ok(new PlayerLoginRes
        {
            Token = token,
            Player = player,
            HubUrl = hubUrl
        });
    }

    private string GenerateJwt(Player player,Table table)
    {
        var jwtKey = _configuration["Jwt:SigningKey"]
                     ?? throw new InvalidOperationException("Jwt:SigningKey is missing.");

        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey));
        var credentials = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

        var claims = new List<Claim>
        {
            new Claim("playerId", player.Id.ToString()),
            new Claim("externalId", player.ExternalId),
            new Claim("type", player.Type),
            new Claim("tableId", table.Id.ToString()),
        };

        var token = new JwtSecurityToken(
            issuer: _configuration["Jwt:Issuer"],
            audience: _configuration["Jwt:Audience"],
            claims: claims,
            expires: DateTime.UtcNow.AddHours(6),
            signingCredentials: credentials
        );

        return new JwtSecurityTokenHandler().WriteToken(token);
    }
    
}
