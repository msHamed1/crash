namespace Crash.Domain.Entities;

public class AppLog
{
    public long Id {set;get;}
    public DateTime CreatedAt {set;get;}
    public string Level {set;get;} = null!;
    public string Message {set;get;} = null!;
    public string Category {set;get;} = null!;
    public string? Exception {set;get;}
}
