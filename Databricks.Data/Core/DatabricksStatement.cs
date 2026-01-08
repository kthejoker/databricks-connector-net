using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Client;
using Databricks.Data.Core.Session;
using Databricks.Data.Log;

namespace Databricks.Data.Core
{
    internal class DatabricksStatement
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksStatement>();

        internal DatabricksSession Session { get; set; }

        private readonly IRestRequester _restRequester;

        private string _lastStatementId = null;

        internal DatabricksStatement(DatabricksSession session)
        {
            Session = session;
            _restRequester = session.RestRequester;
        }

        internal string GetStatementId()
        {
            return _lastStatementId;
        }

        internal DatabricksBaseResultSet Execute(int timeout, string sql, Dictionary<string, object> parameters = null, bool describeOnly = false, bool asyncExec = false)
        {
            return ExecuteInternal(timeout, sql, parameters, describeOnly, asyncExec, CancellationToken.None).Result;
        }

        internal async Task<DatabricksBaseResultSet> ExecuteAsync(int timeout, string sql, Dictionary<string, object> parameters = null, bool describeOnly = false, bool asyncExec = false, CancellationToken cancellationToken = default)
        {
            return await ExecuteInternalAsync(timeout, sql, parameters, describeOnly, asyncExec, cancellationToken);
        }

        private DatabricksRestRequest BuildExecuteStatementRequest(string sql, string warehouseId, string catalog = null, string schema = null)
        {
            var request = new ExecuteStatementRequest
            {
                Statement = sql,
                WarehouseId = warehouseId ?? Session.WarehouseId,
                Catalog = catalog ?? Session.Catalog,
                Schema = schema ?? Session.Schema,
                WaitTimeout = "30s",
                OnWaitTimeout = "CANCEL",
                Format = "ARROW_STREAM"
            };

            var uri = Session.BuildUri("/statements");

            return new DatabricksRestRequest
            {
                Url = uri,
                authorizationToken = Session.Token,
                jsonBody = request,
                sid = Session.SessionId
            };
        }

        private DatabricksRestRequest BuildGetStatementRequest(string statementId)
        {
            var uri = Session.BuildUri($"/statements/{statementId}");

            return new DatabricksRestRequest
            {
                Url = uri,
                authorizationToken = Session.Token,
                sid = Session.SessionId
            };
        }

        private DatabricksRestRequest BuildCancelStatementRequest(string statementId)
        {
            var uri = Session.BuildUri($"/statements/{statementId}/cancel");

            return new DatabricksRestRequest
            {
                Url = uri,
                authorizationToken = Session.Token,
                sid = Session.SessionId
            };
        }

        private async Task<DatabricksBaseResultSet> ExecuteInternalAsync(int timeout, string sql, Dictionary<string, object> parameters, bool describeOnly, bool asyncExec, CancellationToken cancellationToken)
        {
            try
            {
                // Build and send execute statement request
                var executeRequest = BuildExecuteStatementRequest(sql, Session.WarehouseId);
                var executeResponse = await _restRequester.PostAsync<ExecuteStatementResponse>(executeRequest, cancellationToken);

                if (!string.IsNullOrEmpty(executeResponse.ErrorCode))
                {
                    throw new DatabricksDbException(
                        executeResponse.ErrorCode,
                        0,
                        executeResponse.Message ?? "Query execution failed",
                        executeResponse.StatementId ?? string.Empty);
                }

                _lastStatementId = executeResponse.StatementId;

                // If async execution, return immediately with statement ID
                if (asyncExec)
                {
                    return new DatabricksAsyncResultSet(executeResponse.StatementId, this);
                }

                // Poll for results if query is still running
                var statementId = executeResponse.StatementId;
                var status = executeResponse.Status;
                GetStatementResponse getResponse = null;

                while (status != null && (status.State == "PENDING" || status.State == "RUNNING"))
                {
                    await Task.Delay(500, cancellationToken); // Poll every 500ms

                    var getRequest = BuildGetStatementRequest(statementId);
                    getResponse = await _restRequester.GetAsync<GetStatementResponse>(getRequest, cancellationToken);

                    if (!string.IsNullOrEmpty(getResponse.ErrorCode))
                    {
                        throw new DatabricksDbException(
                            getResponse.ErrorCode,
                            0,
                            getResponse.Message ?? "Failed to get statement status",
                            statementId);
                    }

                    status = getResponse.Status;

                    if (status.State == "FAILED" || status.State == "CANCELED")
                    {
                        var errorMsg = status.Error?.Message ?? "Query execution failed";
                        var errorCode = status.Error?.ErrorCode ?? "UNKNOWN";
                        throw new DatabricksDbException(
                            errorCode,
                            0,
                            errorMsg,
                            statementId);
                    }

                    if (status.State == "SUCCEEDED")
                    {
                        return BuildResultSet(getResponse, cancellationToken);
                    }
                }

                // If we exit the loop without success, throw an error
                throw new DatabricksDbException(
                    "QUERY_INCOMPLETE",
                    0,
                    $"Query did not complete. Final state: {status?.State}",
                    statementId);
            }
            catch (Exception ex)
            {
                logger.Error("Query execution failed.", ex);
                throw;
            }
        }

        private Task<DatabricksBaseResultSet> ExecuteInternal(int timeout, string sql, Dictionary<string, object> parameters, bool describeOnly, bool asyncExec, CancellationToken cancellationToken)
        {
            return ExecuteInternalAsync(timeout, sql, parameters, describeOnly, asyncExec, cancellationToken);
        }

        internal DatabricksBaseResultSet BuildResultSet(GetStatementResponse response, CancellationToken cancellationToken)
        {
            if (response.Status?.State == "SUCCEEDED")
            {
                return new DatabricksResultSet(response, this, cancellationToken);
            }
            else if (response.Status?.State == "FAILED")
            {
                var errorMsg = response.Status.Error?.Message ?? "Query execution failed";
                var errorCode = response.Status.Error?.ErrorCode ?? "UNKNOWN";
                throw new DatabricksDbException(
                    errorCode,
                    0,
                    errorMsg,
                    response.StatementId ?? string.Empty);
            }
            else
            {
                throw new DatabricksDbException(
                    "QUERY_INCOMPLETE",
                    0,
                    $"Query is in state: {response.Status?.State}",
                    response.StatementId ?? string.Empty);
            }
        }

        internal DatabricksBaseResultSet BuildResultSet(ExecuteStatementResponse response, CancellationToken cancellationToken)
        {
            if (response.Status?.State == "SUCCEEDED")
            {
                return new DatabricksResultSet(response, this, cancellationToken);
            }
            else if (response.Status?.State == "FAILED")
            {
                var errorMsg = response.Status.Error?.Message ?? "Query execution failed";
                var errorCode = response.Status.Error?.ErrorCode ?? "UNKNOWN";
                throw new DatabricksDbException(
                    errorCode,
                    0,
                    errorMsg,
                    response.StatementId ?? string.Empty);
            }
            else
            {
                throw new DatabricksDbException(
                    "QUERY_INCOMPLETE",
                    0,
                    $"Query is in state: {response.Status?.State}",
                    response.StatementId ?? string.Empty);
            }
        }

        internal void Cancel()
        {
            if (!string.IsNullOrEmpty(_lastStatementId))
            {
                try
                {
                    var cancelRequest = BuildCancelStatementRequest(_lastStatementId);
                    _restRequester.Post<BaseRestResponse>(cancelRequest);
                }
                catch (Exception ex)
                {
                    logger.Error("Failed to cancel statement", ex);
                }
            }
        }
    }
}

