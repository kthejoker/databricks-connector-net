using System;
using System.Data;
using System.Data.Common;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Core;
using Databricks.Data.Core.Session;
using Databricks.Data.Log;

namespace Databricks.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class DatabricksDbConnection : DbConnection
    {
        private DatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksDbConnection>();

        internal DatabricksSession Session { get; set; }

        internal ConnectionState _connectionState;

        protected override DbProviderFactory DbProviderFactory => new DatabricksDbFactory();

        internal int _connectionTimeout;

        private bool _disposed = false;

        public DatabricksDbConnection()
        {
            _connectionState = ConnectionState.Closed;
            _connectionTimeout = 60; // Default 60 seconds
        }

        public DatabricksDbConnection(string connectionString) : this()
        {
            ConnectionString = connectionString;
        }

        public override string ConnectionString
        {
            get; set;
        }

        public SecureString Token
        {
            get; set;
        }

        public bool IsOpen()
        {
            return _connectionState == ConnectionState.Open && Session != null;
        }

        private bool IsNonClosedWithSession()
        {
            return _connectionState != ConnectionState.Closed && Session != null;
        }

        public override string Database => IsOpen() ? Session.Database : string.Empty;

        public override int ConnectionTimeout => this._connectionTimeout;

        public override string DataSource
        {
            get
            {
                return IsOpen() ? Session.ServerHostname : string.Empty;
            }
        }

        public override string ServerVersion => IsOpen() ? Session.ServerVersion : String.Empty;

        public override ConnectionState State => _connectionState;

        internal DatabricksDbTransaction ExplicitTransaction { get; set; }

        public override void ChangeDatabase(string databaseName)
        {
            logger.Debug($"ChangeDatabase to:{databaseName}");

            string alterDbCommand = $"USE {databaseName}";

            using (IDbCommand cmd = CreateCommand())
            {
                cmd.CommandText = alterDbCommand;
                cmd.ExecuteNonQuery();
            }
        }

        public override void Close()
        {
            logger.Debug("Close Connection.");
            if (IsNonClosedWithSession())
            {
                var returnedToPool = TryToReturnSessionToPool();
                if (!returnedToPool)
                {
                    Session.Close();
                }
                Session = null;
            }
            _connectionState = ConnectionState.Closed;
        }

#if NETCOREAPP3_0_OR_GREATER
        public override async Task CloseAsync()
        {
            await CloseAsync(CancellationToken.None);
        }
#endif

        public virtual async Task CloseAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Close Connection.");
            TaskCompletionSource<object> taskCompletionSource = new TaskCompletionSource<object>();

            if (cancellationToken.IsCancellationRequested)
            {
                taskCompletionSource.SetCanceled();
            }
            else
            {
                if (IsNonClosedWithSession())
                {
                    var returnedToPool = TryToReturnSessionToPool();
                    if (returnedToPool)
                    {
                        _connectionState = ConnectionState.Closed;
                        taskCompletionSource.SetResult(null);
                    }
                    else
                    {
                        await Session.CloseAsync(cancellationToken).ContinueWith(
                            previousTask =>
                            {
                                if (previousTask.IsFaulted)
                                {
                                    logger.Error("Error closing the session", previousTask.Exception);
                                    taskCompletionSource.SetException(previousTask.Exception);
                                }
                                else if (previousTask.IsCanceled)
                                {
                                    _connectionState = ConnectionState.Closed;
                                    logger.Debug("Session close canceled");
                                    taskCompletionSource.SetCanceled();
                                }
                                else
                                {
                                    logger.Debug("Session closed successfully");
                                    _connectionState = ConnectionState.Closed;
                                    taskCompletionSource.SetResult(null);
                                }
                            }, cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    logger.Debug("Session not opened. Nothing to do.");
                    taskCompletionSource.SetResult(null);
                }
            }
            await taskCompletionSource.Task;
        }

        private bool TryToReturnSessionToPool()
        {
            var pooling = DatabricksDbConnectionPool.GetPooling() && Session.GetPooling();
            if (!pooling)
            {
                DatabricksDbConnectionPool.ReleaseBusySession(Session);
                return false;
            }
            var sessionReturnedToPool = DatabricksDbConnectionPool.AddSession(Session);
            if (sessionReturnedToPool)
            {
                logger.Debug($"Session pooled: {Session.SessionId}");
            }
            return sessionReturnedToPool;
        }

        public override void Open()
        {
            logger.Debug("Open Connection.");
            if (_connectionState != ConnectionState.Closed)
            {
                logger.Debug($"Open with a connection already opened: {_connectionState}");
                return;
            }
            try
            {
                OnSessionConnecting();
                var sessionContext = new SessionPropertiesContext
                {
                    Token = Token
                };
                Session = DatabricksDbConnectionPool.GetSession(ConnectionString, sessionContext);
                if (Session == null)
                    throw new DatabricksDbException(DatabricksError.INTERNAL_ERROR, "Could not open session");
                logger.Debug($"Connection open with pooled session: {Session.SessionId}");
                OnSessionEstablished();
            }
            catch (Exception e)
            {
                _connectionState = ConnectionState.Closed;
                logger.Error("Unable to connect: ", e);
                if (e is DatabricksDbException)
                {
                    throw;
                }
                throw new DatabricksDbException(
                        e,
                        DatabricksDbException.CONNECTION_FAILURE_SSTATE,
                        DatabricksError.INTERNAL_ERROR,
                        "Unable to connect. " + e.Message);
            }
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Connection Async.");
            if (_connectionState != ConnectionState.Closed)
            {
                logger.Debug($"Open with a connection already opened: {_connectionState}");
                return Task.CompletedTask;
            }
            OnSessionConnecting();
            var sessionContext = new SessionPropertiesContext
            {
                Token = Token
            };
            return DatabricksDbConnectionPool
                .GetSessionAsync(ConnectionString, sessionContext, cancellationToken)
                .ContinueWith(previousTask =>
                {
                    if (previousTask.IsFaulted)
                    {
                        Exception sessionEx = previousTask.Exception;
                        _connectionState = ConnectionState.Closed;
                        logger.Error("Unable to connect", sessionEx);
                        throw new DatabricksDbException(
                           sessionEx,
                           DatabricksDbException.CONNECTION_FAILURE_SSTATE,
                           DatabricksError.INTERNAL_ERROR,
                           "Unable to connect");
                    }
                    else if (previousTask.IsCanceled)
                    {
                        _connectionState = ConnectionState.Closed;
                        logger.Debug("Connection canceled");
                        throw new TaskCanceledException("Connecting was cancelled");
                    }
                    else
                    {
                        Session = previousTask.Result;
                        logger.Debug($"Connection open with pooled session: {Session.SessionId}");
                        OnSessionEstablished();
                    }
                }, TaskContinuationOptions.None);
        }

        private void OnSessionConnecting()
        {
            _connectionState = ConnectionState.Connecting;
        }

        private void OnSessionEstablished()
        {
            _connectionTimeout = (int)Session.ConnectionTimeout.TotalSeconds;
            _connectionState = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (isolationLevel == IsolationLevel.Unspecified)
            {
                isolationLevel = IsolationLevel.ReadCommitted;
            }

            return new DatabricksDbTransaction(isolationLevel, this);
        }

        protected override DbCommand CreateDbCommand()
        {
            var command = DbProviderFactory.CreateCommand();
            command.Connection = this;
            return command;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    try
                    {
                        Close();
                    }
                    catch (Exception ex)
                    {
                        logger.Error("Unable to close connection", ex);
                    }
                }
                else
                {
                    Session?.CloseNonBlocking();
                    Session = null;
                    _connectionState = ConnectionState.Closed;
                }

                _disposed = true;
            }

            base.Dispose(disposing);
        }

        ~DatabricksDbConnection()
        {
            Dispose(false);
        }
    }
}

