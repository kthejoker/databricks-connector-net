using System;
using System.Data;
using System.Data.Common;
using Databricks.Data.Core;
using Databricks.Data.Log;

namespace Databricks.Data.Client
{
    public class DatabricksDbTransaction : DbTransaction
    {
        private IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksDbTransaction>();

        private IsolationLevel isolationLevel;

        private DatabricksDbConnection connection;

        private bool disposed = false;
        private bool isCommittedOrRollbacked = false;

        internal bool IsActive => !disposed && !isCommittedOrRollbacked;

        public DatabricksDbTransaction(IsolationLevel isolationLevel, DatabricksDbConnection connection)
        {
            logger.Debug("Begin transaction.");
            if (isolationLevel != IsolationLevel.ReadCommitted)
            {
                throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
            }
            if (connection == null)
            {
                logger.Error("Transaction cannot be started for an unknown connection");
                throw new DatabricksDbException(DatabricksError.MISSING_CONNECTION_PROPERTY);
            }
            if (!connection.IsOpen())
            {
                logger.Error("Transaction cannot be started for a closed connection");
                throw new DatabricksDbException(DatabricksError.UNSUPPORTED_FEATURE);
            }

            this.isolationLevel = isolationLevel;
            this.connection = connection;

            using (IDbCommand command = connection.CreateCommand())
            {
                isCommittedOrRollbacked = false;
                command.CommandText = "BEGIN TRANSACTION";
                command.ExecuteNonQuery();
            }
            connection.ExplicitTransaction = this;
        }

        public override IsolationLevel IsolationLevel
        {
            get
            {
                return isolationLevel;
            }
        }

        protected override DbConnection DbConnection
        {
            get
            {
                return connection;
            }
        }

        public override void Commit()
        {
            logger.Debug("Commit transaction.");
            if (!isCommittedOrRollbacked)
            {
                using (IDbCommand command = connection.CreateCommand())
                {
                    isCommittedOrRollbacked = true;
                    command.CommandText = "COMMIT";
                    command.ExecuteNonQuery();
                }
            }
        }

        public override void Rollback()
        {
            logger.Debug("Rollback transaction.");
            if (!isCommittedOrRollbacked)
            {
                using (IDbCommand command = connection.CreateCommand())
                {
                    isCommittedOrRollbacked = true;
                    command.CommandText = "ROLLBACK";
                    command.ExecuteNonQuery();
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposed)
                return;
            // Rollback the uncommitted transaction when the connection is open
            if (connection != null && connection.IsOpen())
            {
                if (!isCommittedOrRollbacked)
                {
                    this.Rollback();
                }
                isCommittedOrRollbacked = true;
                connection.ExplicitTransaction = null;
            }
            disposed = true;

            base.Dispose(disposing);
        }

        ~DatabricksDbTransaction()
        {
            Dispose(false);
        }
    }
}

