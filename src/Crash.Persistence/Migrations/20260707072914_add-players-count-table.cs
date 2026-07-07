using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Crash.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class addplayerscounttable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "PlayersCount",
                table: "Tables",
                type: "int",
                nullable: false,
                defaultValue: 0);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "PlayersCount",
                table: "Tables");
        }
    }
}
