using System;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Core;
using Databricks.Data.Log;

namespace Databricks.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class DatabricksDbCommand : DbCommand
    {
        private DatabricksDbConnection connection;

        private DatabricksStatement statement;

        private DatabricksDbParameterCollection parameterCollection;

        private IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksDbCommand>();

        public DatabricksDbCommand()
        {
            logger.Debug("Constructing DatabricksDbCommand class");
            this.CommandTimeout = 0;
            parameterCollection = new DatabricksDbParameterCollection();
        }

        public DatabricksDbCommand(DatabricksDbConnection connection) : this()
        {
            this.connection = connection;
        }

        public DatabricksDbCommand(DatabricksDbConnection connection, string cmdText) : this(connection)
        {
            this.CommandText = cmdText;
        }

        public override string CommandText
        {
            get; set;
        }

        public override int CommandTimeout
        {
            get; set;
        }

        public override CommandType CommandType
        {
            get
            {
                return CommandType.Text;
            }

            set
            {
                if (value != CommandType.Text)
                {
                    throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override bool DesignTimeVisible
        {
            get
            {
                return false;
            }

            set
            {
                if (value)
                {
                    throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get => UpdateRowSource.None;

            set
            {
                if (value != UpdateRowSource.None)
                {
                    throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
                }
            }
        }

        protected override DbConnection DbConnection
        {
            get => connection;

            set
            {
                if (value == null)
                {
                    if (connection == null)
                    {
                        return;
                    }
                    throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
                }

                if (!(value is DatabricksDbConnection))
                {
                    throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
                }

                var dbc = (DatabricksDbConnection)value;
                if (connection != null && connection != dbc)
                {
                    throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
                }

                connection = dbc;
                if (dbc.Session != null)
                {
                    statement = new DatabricksStatement(dbc.Session);
                }
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                return this.parameterCollection;
            }
        }

        protected override DbTransaction DbTransaction
        {
            get;

            set;
        }

        public override void Cancel()
        {
            statement?.Cancel();
        }

        public override int ExecuteNonQuery()
        {
            logger.Debug($"ExecuteNonQuery");
            DatabricksBaseResultSet resultSet = ExecuteInternal();
            int count = resultSet.CalculateUpdateCount();
            return count;
        }

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteNonQueryAsync");
            cancellationToken.ThrowIfCancellationRequested();

            var resultSet = await ExecuteInternalAsync(cancellationToken).ConfigureAwait(false);
            int count = resultSet.CalculateUpdateCount();
            return count;
        }

        public override object ExecuteScalar()
        {
            logger.Debug($"ExecuteScalar");
            DatabricksBaseResultSet resultSet = ExecuteInternal();

            if (resultSet.Next())
                return resultSet.GetValue(0);
            else
                return DBNull.Value;
        }

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteScalarAsync");
            cancellationToken.ThrowIfCancellationRequested();

            var result = await ExecuteInternalAsync(cancellationToken).ConfigureAwait(false);

            if (await result.NextAsync().ConfigureAwait(false))
                return result.GetValue(0);
            else
                return DBNull.Value;
        }

        public override void Prepare()
        {
            // No-op for Databricks
        }

        public string GetStatementId()
        {
            if (statement != null)
            {
                return statement.GetStatementId();
            }
            return null;
        }

        protected override DbParameter CreateDbParameter()
        {
            return new DatabricksDbParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            logger.Debug($"ExecuteDbDataReader");
            DatabricksBaseResultSet resultSet = ExecuteInternal();
            return new DatabricksDbDataReader(this, resultSet);
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteDbDataReaderAsync");
            try
            {
                var result = await ExecuteInternalAsync(cancellationToken).ConfigureAwait(false);
                return new DatabricksDbDataReader(this, result);
            }
            catch (Exception ex)
            {
                logger.Error("The command failed to execute.", ex);
                throw;
            }
        }

        private void SetStatement()
        {
            if (connection == null)
            {
                throw new DatabricksDbException(DatabricksError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);
            }

            var session = (connection as DatabricksDbConnection).Session;

            if (session == null)
                throw new DatabricksDbException(DatabricksError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);

            this.statement = new DatabricksStatement(session);
        }

        private DatabricksBaseResultSet ExecuteInternal(bool describeOnly = false, bool asyncExec = false)
        {
            CheckIfCommandTextIsSet();
            SetStatement();
            // Convert parameters to dictionary (simplified - in production you'd want proper parameter binding)
            Dictionary<string, object> parameters = null;
            if (parameterCollection.Count > 0)
            {
                parameters = new Dictionary<string, object>();
                foreach (DatabricksDbParameter param in parameterCollection)
                {
                    parameters[param.ParameterName] = param.Value;
                }
            }
            return statement.Execute(CommandTimeout, CommandText, parameters, describeOnly, asyncExec);
        }

        private Task<DatabricksBaseResultSet> ExecuteInternalAsync(CancellationToken cancellationToken, bool describeOnly = false, bool asyncExec = false)
        {
            CheckIfCommandTextIsSet();
            SetStatement();
            Dictionary<string, object> parameters = null;
            if (parameterCollection.Count > 0)
            {
                parameters = new Dictionary<string, object>();
                foreach (DatabricksDbParameter param in parameterCollection)
                {
                    parameters[param.ParameterName] = param.Value;
                }
            }
            return statement.ExecuteAsync(CommandTimeout, CommandText, parameters, describeOnly, asyncExec, cancellationToken);
        }

        private void CheckIfCommandTextIsSet()
        {
            if (string.IsNullOrEmpty(CommandText))
            {
                var errorMessage = "Unable to execute command due to command text not being set";
                logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }
    }
}

