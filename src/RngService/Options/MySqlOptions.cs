namespace RngService.Options;

public sealed class MySqlOptions
{
    public const string SectionName = "MySql";

    public string ConnectionString { get; set; } = "";
}
