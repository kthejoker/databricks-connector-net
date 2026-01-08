using System;
using System.Data.Common;
using Databricks.Data.Core;

namespace Databricks.Data.Client
{
    /// <summary>
    ///     Wraps the exception. 
    ///     If the exception is thrown in the client side, error code from 
    ///     270000 to 279999 will be used. Otherwise, server side error code
    ///     will be used. 
    /// </summary>
    public sealed class DatabricksDbException : DbException
    {
        // Sql states not coming directly from the server.
        internal static string CONNECTION_FAILURE_SSTATE = "08006";

        public string SqlState { get; private set; }
        private int VendorCode;

        public string QueryId { get; set; }

        public override int ErrorCode
        {
            get
            {
                return VendorCode;
            }
        }

        public DatabricksDbException(string sqlState, int vendorCode, string errorMessage, string queryId)
            : base(FormatExceptionMessage(errorMessage, vendorCode, sqlState, queryId))
        {
            SqlState = sqlState;
            VendorCode = vendorCode;
            QueryId = queryId;
        }

        public DatabricksDbException(DatabricksError error, string queryId, Exception innerException)
            : base(FormatExceptionMessage(error, innerException?.Message ?? string.Empty, string.Empty, queryId), innerException)
        {
            VendorCode = error.GetAttribute<DatabricksErrorAttr>().errorCode;
            QueryId = queryId;
        }

        public DatabricksDbException(DatabricksError error, params object[] args)
            : base(FormatExceptionMessage(error, string.Join(", ", args), string.Empty, string.Empty))
        {
            VendorCode = error.GetAttribute<DatabricksErrorAttr>().errorCode;
        }

        public DatabricksDbException(string sqlState, DatabricksError error, params object[] args)
            : base(FormatExceptionMessage(error, string.Join(", ", args), sqlState, string.Empty))
        {
            VendorCode = error.GetAttribute<DatabricksErrorAttr>().errorCode;
            SqlState = sqlState;
        }

        public DatabricksDbException(Exception innerException, DatabricksError error, params object[] args)
            : base(FormatExceptionMessage(error, string.Join(", ", args), string.Empty, string.Empty), innerException)
        {
            VendorCode = error.GetAttribute<DatabricksErrorAttr>().errorCode;
        }

        public DatabricksDbException(Exception innerException, string sqlState, DatabricksError error, params object[] args)
            : base(FormatExceptionMessage(error, string.Join(", ", args), sqlState, string.Empty), innerException)
        {
            VendorCode = error.GetAttribute<DatabricksErrorAttr>().errorCode;
            SqlState = sqlState;
        }

        static string FormatExceptionMessage(DatabricksError error,
            string errorMessage,
            string sqlState,
            string queryId)
        {
            return FormatExceptionMessage(errorMessage,
                error.GetAttribute<DatabricksErrorAttr>().errorCode,
                sqlState,
                queryId);
        }

        static string FormatExceptionMessage(string errorMessage,
            int vendorCode,
            string sqlState,
            string queryId)
        {
            return string.Format("Error: {0} SqlState: {1}, VendorCode: {2}, QueryId: {3}",
                errorMessage, sqlState, vendorCode, queryId);
        }
    }
}

