# Databricks .NET Connector - Hello World Example

A simple console application demonstrating basic usage of the Databricks .NET ADO.NET connector.

## Prerequisites

- .NET 8.0 SDK or later
- A Databricks workspace with a SQL warehouse
- A Databricks personal access token

## Building

From the repository root:

```bash
dotnet build Examples/HelloWorld/HelloWorld.csproj
```

Or from this directory:

```bash
cd Examples/HelloWorld
dotnet build
```

## Running

### Option 1: Update connection string in code

Edit `Program.cs` and update the connection string with your Databricks workspace details:

```csharp
string connectionString = "SERVER_HOSTNAME=https://your-workspace.databricks.com;" +
                         "HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id;" +
                         "TOKEN=your-token-here;" +
                         "CATALOG=default_catalog;" +
                         "SCHEMA=default_schema";
```

Then run:

```bash
dotnet run
```

### Option 2: Pass connection string as argument

```bash
dotnet run "SERVER_HOSTNAME=https://adb-1234567890123456.7.databricks.azure.com;HTTP_PATH=/sql/1.0/warehouses/abc123def456;TOKEN=your-token;CATALOG=default_catalog;SCHEMA=default_schema"
```

## Connection String Parameters

- **SERVER_HOSTNAME** (required): Your Databricks workspace URL
- **HTTP_PATH** (required): The SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/abc123def456`)
- **TOKEN** (required): Personal access token or OAuth token
- **CATALOG** (optional): Default catalog name
- **SCHEMA** (optional): Default schema name

## What This Example Does

1. Creates a connection to Databricks
2. Opens the connection
3. Executes a simple SELECT query: `SELECT 1 as hello, 'World' as message`
4. Reads and displays the results
5. Closes the connection

## Expected Output

```
Databricks .NET Connector - Hello World Example
================================================

Connecting to Databricks...
✓ Connected successfully!
  Server Version: 1.0.0
  Database: default_catalog

Executing query: SELECT 1 as hello, 'World' as message
✓ Query executed successfully!
  Results:
    hello: 1
    message: World

Hello World from Databricks! 🎉
```

