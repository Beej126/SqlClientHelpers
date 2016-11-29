using System;
using NextTech.SqlClientHelpers;

namespace TestConsole
{
  class Program
  {
    public static string Env(string name) => Environment.GetEnvironmentVariable(name);
    static async void Main(string[] args)
    {
      Proc.ConnectionStringDefault = Env("SqlClientHelpers_TestConnectionString");
      Proc.OnSuccessDefault = () => Console.WriteLine("\r\nSuccess!\r\n");
      Proc.OnErrorDefault = (ex) => Console.WriteLine($"\r\nException: {ex.Message}\r\n");

      using (var proc = new Proc("dbo.ProdDetail_mobile"))
      {
        proc["@ItemId"] = 1000024;
        var stdout = Console.OpenStandardOutput();
        
        //proc.ExecuteDataSetAsync().Wait();
        (await proc.ExecuteReaderAsync()).ToJsonAsync(stdout).Wait();
      }
      Console.WriteLine("\r\n\r\nPress any key to end and close");
      Console.Read();
    }
  }
}
