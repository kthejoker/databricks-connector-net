using System.Data.Common;

namespace Databricks.Data.Client
{
    public sealed class DatabricksDbFactory : DbProviderFactory
    {
        public static readonly DatabricksDbFactory Instance = new DatabricksDbFactory();

        /// <summary>
        /// Returns a strongly typed <see cref="DbCommand"/> instance.
        /// </summary>
        public override DbCommand CreateCommand() => new DatabricksDbCommand();

        /// <summary>
        /// Returns a strongly typed <see cref="DbConnection"/> instance.
        /// </summary>
        public override DbConnection CreateConnection() => new DatabricksDbConnection();

        /// <summary>
        /// Returns a strongly typed <see cref="DbParameter"/> instance.
        /// </summary>
        public override DbParameter CreateParameter() => new DatabricksDbParameter();

        /// <summary>
        /// Returns a strongly typed <see cref="DbConnectionStringBuilder"/> instance.
        /// </summary>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DatabricksDbConnectionStringBuilder();

        /// <summary>
        /// Returns a strongly typed <see cref="DbCommandBuilder"/> instance.
        /// </summary>
        public override DbCommandBuilder CreateCommandBuilder() => new DatabricksDbCommandBuilder();

        /// <summary>
        /// Returns a strongly typed <see cref="DbDataAdapter"/> instance.
        /// </summary>
        public override DbDataAdapter CreateDataAdapter() => new DatabricksDbDataAdapter();
    }
}

