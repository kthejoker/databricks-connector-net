using System;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Core;
using Databricks.Data.Core.Session;
using Databricks.Data.Log;

namespace Databricks.Data.Client
{
    public class DatabricksDbConnectionPool
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksDbConnectionPool>();
        private static readonly object s_connectionManagerInstanceLock = new object();
        private static IConnectionManager s_connectionManager;
        private static bool s_poolingEnabled = true;

        internal static IConnectionManager ConnectionManager
        {
            get
            {
                if (s_connectionManager != null)
                    return s_connectionManager;
                lock (s_connectionManagerInstanceLock)
                {
                    if (s_connectionManager == null)
                    {
                        s_connectionManager = new ConnectionManager();
                    }
                }
                return s_connectionManager;
            }
        }

        internal static DatabricksSession GetSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            logger.Debug($"DatabricksDbConnectionPool::GetSession");
            return ConnectionManager.GetSession(connectionString, sessionContext);
        }

        internal static Task<DatabricksSession> GetSessionAsync(string connectionString, SessionPropertiesContext sessionContext, CancellationToken cancellationToken)
        {
            logger.Debug($"DatabricksDbConnectionPool::GetSessionAsync");
            return ConnectionManager.GetSessionAsync(connectionString, sessionContext, cancellationToken);
        }

        internal static bool AddSession(DatabricksSession session)
        {
            logger.Debug("DatabricksDbConnectionPool::AddSession");
            return ConnectionManager.AddSession(session);
        }

        internal static void ReleaseBusySession(DatabricksSession session)
        {
            logger.Debug("DatabricksDbConnectionPool::ReleaseBusySession");
            ConnectionManager.ReleaseBusySession(session);
        }

        public static void ClearAllPools()
        {
            logger.Debug("DatabricksDbConnectionPool::ClearAllPools");
            ConnectionManager.ClearAllPools();
        }

        public static bool GetPooling()
        {
            return s_poolingEnabled;
        }

        public static void SetPooling(bool enabled)
        {
            s_poolingEnabled = enabled;
        }
    }

    internal interface IConnectionManager
    {
        DatabricksSession GetSession(string connectionString, SessionPropertiesContext sessionContext);
        Task<DatabricksSession> GetSessionAsync(string connectionString, SessionPropertiesContext sessionContext, CancellationToken cancellationToken);
        bool AddSession(DatabricksSession session);
        void ReleaseBusySession(DatabricksSession session);
        void ClearAllPools();
    }

    internal class ConnectionManager : IConnectionManager
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<ConnectionManager>();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, System.Collections.Concurrent.ConcurrentQueue<DatabricksSession>> _pools = 
            new System.Collections.Concurrent.ConcurrentDictionary<string, System.Collections.Concurrent.ConcurrentQueue<DatabricksSession>>();

        public DatabricksSession GetSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            if (!DatabricksDbConnectionPool.GetPooling())
            {
                return CreateNewSession(connectionString, sessionContext);
            }

            var poolKey = GetPoolKey(connectionString, sessionContext);
            if (_pools.TryGetValue(poolKey, out var pool))
            {
                if (pool.TryDequeue(out var session))
                {
                    logger.Debug($"Reusing pooled session: {session.SessionId}");
                    return session;
                }
            }

            return CreateNewSession(connectionString, sessionContext);
        }

        public async Task<DatabricksSession> GetSessionAsync(string connectionString, SessionPropertiesContext sessionContext, CancellationToken cancellationToken)
        {
            if (!DatabricksDbConnectionPool.GetPooling())
            {
                var session = CreateNewSession(connectionString, sessionContext);
                await session.OpenAsync(cancellationToken);
                return session;
            }

            var poolKey = GetPoolKey(connectionString, sessionContext);
            if (_pools.TryGetValue(poolKey, out var pool))
            {
                if (pool.TryDequeue(out var session))
                {
                    logger.Debug($"Reusing pooled session: {session.SessionId}");
                    return session;
                }
            }

            var newSession = CreateNewSession(connectionString, sessionContext);
            await newSession.OpenAsync(cancellationToken);
            return newSession;
        }

        public bool AddSession(DatabricksSession session)
        {
            if (!DatabricksDbConnectionPool.GetPooling() || !session.GetPooling())
            {
                return false;
            }

            var poolKey = GetPoolKey(session.ConnectionString, session.PropertiesContext);
            var pool = _pools.GetOrAdd(poolKey, _ => new System.Collections.Concurrent.ConcurrentQueue<DatabricksSession>());
            
            // Simple pool size limit (can be made configurable)
            if (pool.Count < 10)
            {
                pool.Enqueue(session);
                return true;
            }

            return false;
        }

        public void ReleaseBusySession(DatabricksSession session)
        {
            // Clean up if needed
            session?.Close();
        }

        public void ClearAllPools()
        {
            foreach (var pool in _pools.Values)
            {
                while (pool.TryDequeue(out var session))
                {
                    session.Close();
                }
            }
            _pools.Clear();
        }

        private DatabricksSession CreateNewSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            var session = new DatabricksSession(connectionString, sessionContext);
            session.Open();
            return session;
        }

        private string GetPoolKey(string connectionString, SessionPropertiesContext sessionContext)
        {
            // Create a key based on connection string (excluding token)
            // This is simplified - in production you'd want a more robust key
            // Remove token from connection string for key generation
            if (string.IsNullOrEmpty(connectionString))
                return "default";
            
            // Simple hash-based key (in production, parse and exclude secrets)
            return connectionString.GetHashCode().ToString();
        }
    }
}

