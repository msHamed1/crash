namespace Crash.Domain.Entities;

public class Owner
{
    public long Id { get; set; }
    public string Name { get; set; }
    public List<Table> Tables { get; set; }
    
 }