using System;
using System.Diagnostics;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Client;
using Databricks.Data.Log;

namespace Databricks.Data.Core
{
    /// <summary>
    /// The RestRequester is responsible to send out a rest request and receive response
    /// </summary>
    internal interface IRestRequester
    {
        Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken);

        T Post<T>(IRestRequest postRequest);

        Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken);

        T Get<T>(IRestRequest request);

        Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken);

        HttpResponseMessage Get(IRestRequest request);
    }

    internal interface IMockRestRequester : IRestRequester
    {
        void setHttpClient(HttpClient httpClient);
    }

    internal class RestRequester : IRestRequester
    {
        private static IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<RestRequester>();

        protected HttpClient _HttpClient;

        public RestRequester(HttpClient httpClient)
        {
            _HttpClient = httpClient;
        }

        public T Post<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (PostAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public async Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            using (var response = await SendAsync(HttpMethod.Post, request, cancellationToken).ConfigureAwait(false))
            {
                var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                logger.Debug($"POST Response Status: {response.StatusCode}");
                logger.Debug($"POST Response Body: {json}");
                return JsonConvert.DeserializeObject<T>(json, JsonUtils.JsonSettings);
            }
        }

        public T Get<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public async Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            using (HttpResponseMessage response = await GetAsync(request, cancellationToken).ConfigureAwait(false))
            {
                var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                logger.Debug($"GET Response Status: {response.StatusCode}");
                logger.Debug($"GET Response Body: {json}");
                return JsonConvert.DeserializeObject<T>(json, JsonUtils.JsonSettings);
            }
        }

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            return SendAsync(HttpMethod.Get, request, cancellationToken);
        }

        public HttpResponseMessage Get(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        private async Task<HttpResponseMessage> SendAsync(HttpMethod method,
                                                          IRestRequest request,
                                                          CancellationToken externalCancellationToken)
        {
            // Log request details before creating HttpRequestMessage
            if (request is DatabricksRestRequest dbRequest)
            {
                logger.Debug($"{method} Request URL: {dbRequest.Url}");
                if (dbRequest.jsonBody != null)
                {
                    var requestBody = JsonConvert.SerializeObject(dbRequest.jsonBody, JsonUtils.JsonSettings);
                    logger.Debug($"{method} Request Body: {requestBody}");
                }
                if (!string.IsNullOrEmpty(dbRequest.authorizationToken))
                {
                    var tokenPreview = dbRequest.authorizationToken.Length > 20 
                        ? dbRequest.authorizationToken.Substring(0, 20) + "..." 
                        : dbRequest.authorizationToken;
                    logger.Debug($"{method} Authorization: Bearer {tokenPreview}");
                }
            }
            
            using (HttpRequestMessage message = request.ToRequestMessage(method))
            {
                return await SendAsync(message, request.GetRestTimeout(), externalCancellationToken, request.getSid()).ConfigureAwait(false);
            }
        }

        protected virtual async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout,
                                                              CancellationToken externalCancellationToken,
                                                              string sid = "")
        {
            // merge multiple cancellation token
            using (CancellationTokenSource restRequestTimeout = new CancellationTokenSource(restTimeout))
            {
                using (CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken,
                restRequestTimeout.Token))
                {
                    HttpResponseMessage response = null;
                    logger.Debug($"Executing: {sid} {message.Method} {message.RequestUri}");
                    logger.Debug($"Request Headers:");
                    foreach (var header in message.Headers)
                    {
                        logger.Debug($"  {header.Key}: {string.Join(", ", header.Value)}");
                    }
                    if (message.Content != null)
                    {
                        logger.Debug($"Content-Type: {message.Content.Headers.ContentType?.MediaType}");
                    }
                    var watch = new Stopwatch();
                    try
                    {
                        watch.Start();
                        response = await _HttpClient
                            .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, linkedCts.Token)
                            .ConfigureAwait(false);
                        watch.Stop();
                        if (!response.IsSuccessStatusCode)
                        {
                            logger.Error($"Failed response after {watch.ElapsedMilliseconds} ms: {sid} {message.Method} {message.RequestUri} StatusCode: {(int)response.StatusCode}, ReasonPhrase: '{response.ReasonPhrase}'");
                        }
                        else
                        {
                            logger.Debug($"Succeeded response after {watch.ElapsedMilliseconds} ms: {sid} {message.Method} {message.RequestUri}");
                        }
                        response.EnsureSuccessStatusCode();

                        return response;
                    }
                    catch (Exception e)
                    {
                        if (watch.IsRunning)
                        {
                            watch.Stop();
                            logger.Error($"Response receiving interrupted by exception after {watch.ElapsedMilliseconds} ms. {sid} {message.Method} {message.RequestUri}");
                        }
                        // Disposing of the response if not null now that we don't need it anymore
                        response?.Dispose();
                        if (restRequestTimeout.IsCancellationRequested)
                        {
                            throw new DatabricksDbException(e, DatabricksError.REQUEST_TIMEOUT);
                        }
                        throw;
                    }
                }
            }
        }
    }
}

