using System;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Crash.Persistence.Migrations;

[DbContext(typeof(DataContext))]
[Migration("20260716150000_AddDbWorkerInbox")]
public sealed class AddDbWorkerInbox : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<long>(
            name: "PersistenceSequence",
            table: "Bets",
            type: "bigint",
            nullable: false,
            defaultValue: 0L);

        migrationBuilder.CreateTable(
            name: "ProcessedDbMessages",
            columns: table => new
            {
                MessageId = table.Column<Guid>(
                    type: "char(36)",
                    nullable: false,
                    collation: "ascii_general_ci"),
                MessageType = table.Column<string>(
                    type: "varchar(100)",
                    maxLength: 100,
                    nullable: false)
                    .Annotation("MySql:CharSet", "utf8mb4"),
                TableId = table.Column<long>(type: "bigint", nullable: false),
                RoundId = table.Column<long>(type: "bigint", nullable: false),
                Sequence = table.Column<long>(type: "bigint", nullable: false),
                ProcessedAt = table.Column<DateTimeOffset>(
                    type: "datetime(6)",
                    nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_ProcessedDbMessages", x => x.MessageId);
            })
            .Annotation("MySql:CharSet", "utf8mb4");

        migrationBuilder.CreateIndex(
            name: "IX_ProcessedDbMessages_TableId_RoundId_Sequence",
            table: "ProcessedDbMessages",
            columns: new[] { "TableId", "RoundId", "Sequence" });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "ProcessedDbMessages");

        migrationBuilder.DropColumn(
            name: "PersistenceSequence",
            table: "Bets");
    }
}
