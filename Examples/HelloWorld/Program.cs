using System;
using System.Data;
using Databricks.Data.Client;
using Databricks.Data.Log;

namespace HelloWorld
{
    class Program
    {
        static void Main(string[] args)
        {
            // Enable console logging for the Databricks connector
            DatabricksLogger.EnableConsoleOutput(true);
            
            Console.WriteLine("Databricks .NET Connector - Hello World Example");
            Console.WriteLine("================================================");
            Console.WriteLine();

            // Connection string - update these values with your Databricks workspace details
            string connectionString = "SERVER_HOSTNAME=<server>.azuredatabricks.net/;" +
                                     "HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>;" +
                                     "TOKEN=<token>;" +
                                     "CATALOG=<catalog>;" +
                                     "SCHEMA=<schema>";

            // Check if connection string was provided via command line
            if (args.Length > 0)
            {
                connectionString = args[0];
            }

            try
            {
                // Create and open connection
                Console.WriteLine("Connecting to Databricks...");
                using (var conn = new DatabricksDbConnection(connectionString))
                {
                    conn.Open();
                    Console.WriteLine("✓ Connected successfully!");
                    Console.WriteLine($"  Server Version: {conn.ServerVersion}");
                    Console.WriteLine($"  Database: {conn.Database}");
                    Console.WriteLine();

                    // Execute a simple query
                    Console.WriteLine("Executing query: SELECT 1 as hello, 'World' as message");
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT 1 as hello, 'World' as message";
                        
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                Console.WriteLine("✓ Query executed successfully!");
                                Console.WriteLine($"  Results:");
                                Console.WriteLine($"    hello: {reader.GetInt32(0)}");
                                Console.WriteLine($"    message: {reader.GetString(1)}");
                            }
                        }
                    }

                    Console.WriteLine();
                    Console.WriteLine("Hello World from Databricks! 🎉");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error: {ex.Message}");
                Console.WriteLine();
                Console.WriteLine("Usage:");
                Console.WriteLine("  dotnet run");
                Console.WriteLine("  dotnet run \"SERVER_HOSTNAME=...;HTTP_PATH=...;TOKEN=...\"");
                Console.WriteLine();
                Console.WriteLine("Connection String Format:");
                Console.WriteLine("  SERVER_HOSTNAME=<workspace-url>;");
                Console.WriteLine("  HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>;");
                Console.WriteLine("  TOKEN=<access-token>;");
                Console.WriteLine("  CATALOG=<catalog>;");
                Console.WriteLine("  SCHEMA=<schema>");
                Environment.Exit(1);
            }
        }
    }
}

