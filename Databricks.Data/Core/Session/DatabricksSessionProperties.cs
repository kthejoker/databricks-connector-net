using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.RegularExpressions;
using Databricks.Data.Client;
using Databricks.Data.Log;

namespace Databricks.Data.Core.Session
{
    internal class DatabricksSessionProperties : Dictionary<DatabricksSessionProperty, string>
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksSessionProperties>();

        internal string ConnectionStringWithoutSecrets { get; set; }

        internal static DatabricksSessionProperties ParseConnectionString(string connectionString, SessionPropertiesContext propertiesContext)
        {
            logger.Info("Start parsing connection string.");
            var builder = new DbConnectionStringBuilder();
            try
            {
                builder.ConnectionString = connectionString;
            }
            catch (ArgumentException e)
            {
                logger.Error("Invalid connectionString", e);
                throw new DatabricksDbException(e,
                                DatabricksError.INVALID_CONNECTION_STRING,
                                e.Message);
            }
            var properties = new DatabricksSessionProperties();

            var keys = new string[builder.Keys.Count];
            var values = new string[builder.Values.Count];
            builder.Keys.CopyTo(keys, 0);
            builder.Values.CopyTo(values, 0);

            for (var i = 0; i < keys.Length; i++)
            {
                try
                {
                    DatabricksSessionProperty p = (DatabricksSessionProperty)Enum.Parse(
                                typeof(DatabricksSessionProperty), keys[i].ToUpper());
                    properties.Add(p, values[i]);
                }
                catch (ArgumentException)
                {
                    logger.Debug($"Property {keys[i]} not found - ignored.");
                }
            }

            propertiesContext.FillSecrets(properties);
            CheckSessionProperties(properties);

            return properties;
        }

        private static void CheckSessionProperties(DatabricksSessionProperties properties)
        {
            // Check required properties
            if (!properties.ContainsKey(DatabricksSessionProperty.SERVER_HOSTNAME))
            {
                throw new DatabricksDbException(DatabricksError.MISSING_CONNECTION_PROPERTY, "SERVER_HOSTNAME");
            }

            if (!properties.ContainsKey(DatabricksSessionProperty.HTTP_PATH))
            {
                throw new DatabricksDbException(DatabricksError.MISSING_CONNECTION_PROPERTY, "HTTP_PATH");
            }

            // Token is required if not provided via SecureString
            if (!properties.ContainsKey(DatabricksSessionProperty.TOKEN))
            {
                throw new DatabricksDbException(DatabricksError.MISSING_CONNECTION_PROPERTY, "TOKEN");
            }
        }
    }

    internal enum DatabricksSessionProperty
    {
        SERVER_HOSTNAME,    // Databricks workspace URL (e.g., https://adb-1234567890123456.7.databricks.azure.com)
        HTTP_PATH,          // SQL warehouse HTTP path (e.g., /sql/1.0/warehouses/abc123def456)
        TOKEN,              // Personal access token or OAuth token
        CATALOG,            // Catalog name
        SCHEMA,             // Schema name
        WAREHOUSE_ID,       // SQL warehouse ID
        CONNECTION_TIMEOUT, // Connection timeout in seconds
        QUERY_TIMEOUT,      // Query timeout in seconds
        MAX_RETRIES,        // Maximum number of retries
        RETRY_TIMEOUT       // Retry timeout in seconds
    }
}

