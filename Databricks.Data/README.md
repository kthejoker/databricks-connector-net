# Databricks Connector for .NET

A .NET ADO.NET provider for Databricks that uses the Databricks SQL API as the underlying connection source.

## Overview

This connector provides a standard ADO.NET interface for connecting to and querying Databricks SQL warehouses using the Databricks SQL API (REST API). It implements the standard ADO.NET interfaces, making it easy to integrate with existing .NET applications and frameworks.

## Features

- **ADO.NET Compatible**: Implements standard ADO.NET interfaces (`DbConnection`, `DbCommand`, `DbDataReader`, etc.)
- **Connection Pooling**: Built-in connection pooling support
- **Async/Await Support**: Full async/await support for all operations
- **Parameter Binding**: Support for parameterized queries
- **Transaction Support**: Basic transaction support
- **Token-based Authentication**: Uses Databricks personal access tokens or OAuth tokens

## Installation

```bash
dotnet add package Databricks.Data
```

## Connection String

The connection string uses the following format:

```
SERVER_HOSTNAME=<workspace-url>;HTTP_PATH=<sql-warehouse-path>;TOKEN=<access-token>;WAREHOUSE_ID=<warehouse-id>;CATALOG=<catalog>;SCHEMA=<schema>
```

### Connection String Parameters

- **SERVER_HOSTNAME** (required): Your Databricks workspace URL (e.g., `https://adb-1234567890123456.7.databricks.azure.com`)
- **HTTP_PATH** (required): The SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/abc123def456`)
- **TOKEN** (required): Personal access token or OAuth token
- **WAREHOUSE_ID** (optional): SQL warehouse ID (can also be specified in HTTP_PATH)
- **CATALOG** (optional): Default catalog name
- **SCHEMA** (optional): Default schema name
- **CONNECTION_TIMEOUT** (optional): Connection timeout in seconds (default: 60)
- **QUERY_TIMEOUT** (optional): Query timeout in seconds (default: 0 = no timeout)

## Usage Examples

### Basic Connection and Query

```csharp
using Databricks.Data.Client;
using System.Data;

// Create connection
using (var conn = new DatabricksDbConnection())
{
    conn.ConnectionString = "SERVER_HOSTNAME=https://adb-1234567890123456.7.databricks.azure.com;" +
                           "HTTP_PATH=/sql/1.0/warehouses/abc123def456;" +
                           "TOKEN=your-token-here;" +
                           "CATALOG=default_catalog;" +
                           "SCHEMA=default_schema";
    
    conn.Open();
    
    // Execute query
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = "SELECT * FROM my_table LIMIT 10";
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            }
        }
    }
}
```

### Using SecureString for Token

```csharp
using System.Security;

var conn = new DatabricksDbConnection("SERVER_HOSTNAME=...;HTTP_PATH=...");
var secureToken = new SecureString();
foreach (char c in "your-token-here")
{
    secureToken.AppendChar(c);
}
conn.Token = secureToken;
conn.Open();
```

### Parameterized Queries

```csharp
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT * FROM my_table WHERE id = @id AND name = @name";
    
    var idParam = new DatabricksDbParameter("@id", DbType.Int32);
    idParam.Value = 123;
    cmd.Parameters.Add(idParam);
    
    var nameParam = new DatabricksDbParameter("@name", DbType.String);
    nameParam.Value = "John";
    cmd.Parameters.Add(nameParam);
    
    using (var reader = cmd.ExecuteReader())
    {
        // Process results
    }
}
```

### Async Operations

```csharp
using (var conn = new DatabricksDbConnection(connectionString))
{
    await conn.OpenAsync();
    
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = "SELECT * FROM my_table";
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                Console.WriteLine(reader.GetString(0));
            }
        }
    }
}
```

### Connection Pooling

Connection pooling is enabled by default. You can control it:

```csharp
// Disable pooling globally
DatabricksDbConnectionPool.SetPooling(false);

// Clear all pools
DatabricksDbConnectionPool.ClearAllPools();
```

## Architecture

The connector is organized into three main layers:

- **Client Layer**: `DatabricksDbConnection`, `DatabricksDbCommand`, `DatabricksDbDataReader` - Standard ADO.NET interfaces
- **Core Layer**: `DatabricksSession`, `DatabricksStatement`, `DatabricksBaseResultSet` - Core functionality and query execution
- **REST Layer**: `RestRequester`, `DatabricksRestRequest`, REST API response models - HTTP communication with Databricks SQL API

## Features and Capabilities

- **Authentication**: Token-based authentication using Databricks personal access tokens or OAuth tokens
- **API Endpoints**: Uses Databricks SQL API v2.0 endpoints
- **Query Execution**: Supports synchronous and asynchronous query execution with polling for long-running queries
- **Result Format**: Supports Databricks result formats (ARROW_STREAM, JSON_ARRAY)
- **Connection Management**: Built-in connection pooling for efficient resource usage

## Limitations

- Parameter binding uses standard ADO.NET parameter binding
- Transaction support is basic (BEGIN/COMMIT/ROLLBACK)
- Query cancellation requires statement ID tracking

## Requirements

- .NET Framework 4.6.2 or later
- .NET Standard 2.0 or later
- .NET 6.0 or later

## License

Apache 2.0

## Contributing

Contributions are welcome! Please see the contributing guidelines for more information.

