using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Crash.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class add_bet_persistance : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "IsPersisted",
                table: "Bets",
                type: "tinyint(1)",
                nullable: false,
                defaultValue: false);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "IsPersisted",
                table: "Bets");

            migrationBuilder.AlterColumn<Guid>(
                name: "MessageId",
                table: "ProcessedDbMessages",
                type: "char(36)",
                nullable: false,
                collation: "ascii_general_ci",
                oldClrType: typeof(Guid),
                oldType: "char(36)")
                .OldAnnotation("Relational:Collation", "ascii_general_ci");
        }
    }
}
