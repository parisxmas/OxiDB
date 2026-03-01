namespace OxiDb.Tests.EFCore.Models;

public class Employee
{
    public string Id { get; set; } = null!;
    public string FirstName { get; set; } = null!;
    public string LastName { get; set; } = null!;
    public string Email { get; set; } = null!;
    public string Department { get; set; } = null!;
    public string City { get; set; } = null!;
    public string Country { get; set; } = null!;
    public string Status { get; set; } = null!;
    public int Age { get; set; }
    public int Seq { get; set; }
    public double Salary { get; set; }
    public double Score { get; set; }
    public bool Verified { get; set; }
    public int Rating { get; set; }
    public List<string> Tags { get; set; } = new();
}
