using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Client;
using Databricks.Data.Log;

namespace Databricks.Data.Core.Session
{
    internal class DatabricksSession
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksSession>();

        internal string SessionId { get; set; }
        internal string Token { get; private set; }
        internal string ServerHostname { get; private set; }
        internal string HttpPath { get; private set; }
        internal string WarehouseId { get; private set; }
        internal string Catalog { get; set; }
        internal string Schema { get; set; }
        internal string ServerVersion { get; set; } = "1.0.0";

        internal IRestRequester RestRequester { get; private set; }

        internal DatabricksSessionProperties Properties { get; private set; }

        private HttpClient _HttpClient;
        private bool _pooling = true;

        internal TimeSpan ConnectionTimeout { get; private set; }

        internal string ConnectionString { get; set; }
        internal SessionPropertiesContext PropertiesContext { get; set; }

        internal DatabricksSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            ConnectionString = connectionString;
            PropertiesContext = sessionContext;
            Properties = DatabricksSessionProperties.ParseConnectionString(connectionString, sessionContext);

            ServerHostname = Properties[DatabricksSessionProperty.SERVER_HOSTNAME];
            HttpPath = Properties[DatabricksSessionProperty.HTTP_PATH];
            Token = Properties[DatabricksSessionProperty.TOKEN];

            // Extract warehouse ID from HTTP_PATH if not explicitly provided
            // HTTP_PATH format: /sql/1.0/warehouses/{warehouse-id}
            if (Properties.TryGetValue(DatabricksSessionProperty.WAREHOUSE_ID, out var warehouseId))
            {
                WarehouseId = warehouseId;
            }
            else if (!string.IsNullOrEmpty(HttpPath))
            {
                // Try to extract warehouse ID from HTTP_PATH
                var parts = HttpPath.Split('/');
                for (int i = 0; i < parts.Length - 1; i++)
                {
                    if (parts[i] == "warehouses" && i + 1 < parts.Length)
                    {
                        WarehouseId = parts[i + 1];
                        break;
                    }
                }
            }

            if (Properties.TryGetValue(DatabricksSessionProperty.CATALOG, out var catalog))
            {
                Catalog = catalog;
            }

            if (Properties.TryGetValue(DatabricksSessionProperty.SCHEMA, out var schema))
            {
                Schema = schema;
            }

            var connectionTimeoutSeconds = 60;
            if (Properties.TryGetValue(DatabricksSessionProperty.CONNECTION_TIMEOUT, out var timeoutStr))
            {
                if (int.TryParse(timeoutStr, out var timeout))
                {
                    connectionTimeoutSeconds = timeout;
                }
            }
            ConnectionTimeout = TimeSpan.FromSeconds(connectionTimeoutSeconds);

            // Initialize HTTP client
            _HttpClient = new HttpClient
            {
                Timeout = ConnectionTimeout
            };
            _HttpClient.DefaultRequestHeaders.Add("User-Agent", "Databricks.Data/1.0.0");

            RestRequester = new RestRequester(_HttpClient);

            // Generate a session ID
            SessionId = Guid.NewGuid().ToString();
        }

        internal void Open()
        {
            logger.Debug("Open Session");
            // Databricks doesn't require explicit session establishment
            // The token is used for each request
            // We can verify connectivity by making a simple request if needed
        }

        internal async Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Session Async");
            // Databricks doesn't require explicit session establishment
            await Task.CompletedTask;
        }

        internal bool IsEstablished()
        {
            return !string.IsNullOrEmpty(Token) && !string.IsNullOrEmpty(ServerHostname);
        }

        internal void Close()
        {
            logger.Debug($"Closing session with id: {SessionId}");
            _HttpClient?.Dispose();
        }

        internal void CloseNonBlocking()
        {
            logger.Debug($"Closing session with id: {SessionId}");
            Task.Run(() => _HttpClient?.Dispose());
        }

        internal async Task CloseAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"Closing session with id: {SessionId}");
            _HttpClient?.Dispose();
            await Task.CompletedTask;
        }

        internal Uri BuildUri(string path)
        {
            var baseUrl = ServerHostname.TrimEnd('/');
            if (!baseUrl.StartsWith("http://") && !baseUrl.StartsWith("https://"))
            {
                baseUrl = "https://" + baseUrl;
            }
            // Databricks SQL Statement Execution API v2.0 endpoints:
            // Execute: POST /api/2.0/sql/statements
            // Get status: GET /api/2.0/sql/statements/{statement_id}
            // Cancel: POST /api/2.0/sql/statements/{statement_id}/cancel
            // path should be like "" (empty for execute), "/{statement_id}", or "/{statement_id}/cancel"
            return new Uri($"{baseUrl}/api/2.0/sql/statements{path}");
        }

        internal bool GetPooling()
        {
            return _pooling;
        }

        internal void SetPooling(bool pooling)
        {
            _pooling = pooling;
        }
    }
}

