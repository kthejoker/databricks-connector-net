using System.Net.Http;
using System.Net.Http.Headers;
using System;
using System.Text;
using Newtonsoft.Json;

namespace Databricks.Data.Core
{
    internal interface IRestRequest
    {
        HttpRequestMessage ToRequestMessage(HttpMethod method);
        TimeSpan GetRestTimeout();
        string getSid();
    }

    /// <summary>
    /// A base rest request implementation with timeout defined
    /// </summary>
    internal abstract class BaseRestRequest : IRestRequest
    {
        internal static string HTTP_REQUEST_TIMEOUT_KEY = "TIMEOUT_PER_HTTP_REQUEST";
        internal static string REST_REQUEST_TIMEOUT_KEY = "TIMEOUT_PER_REST_REQUEST";

        // The default Rest timeout. Set to 120 seconds.
        public static readonly int s_defaultRestRetrySecondsTimeout = 120;

        // Default each http request timeout to 16 seconds
        public static readonly int s_defaultHttpSecondsTimeout = 16;

        internal Uri Url { get; set; }

        /// <summary>
        /// Timeout of the overall rest request
        /// </summary>
        internal TimeSpan RestTimeout { get; set; }

        internal String sid { get; set; }

        /// <summary>
        /// Timeout for every single HTTP request
        /// </summary>
        internal TimeSpan HttpTimeout { get; set; }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            throw new NotImplementedException();
        }

        protected HttpRequestMessage newMessage(HttpMethod method, Uri url)
        {
            HttpRequestMessage message = new HttpRequestMessage(method, url);
            message.Properties[HTTP_REQUEST_TIMEOUT_KEY] = HttpTimeout;
            message.Properties[REST_REQUEST_TIMEOUT_KEY] = RestTimeout;
            return message;
        }

        TimeSpan IRestRequest.GetRestTimeout()
        {
            return RestTimeout;
        }

        string IRestRequest.getSid()
        {
            return sid;
        }
    }

    internal class DatabricksRestRequest : BaseRestRequest, IRestRequest
    {
        private static MediaTypeWithQualityHeaderValue applicationJson = new MediaTypeWithQualityHeaderValue("application/json");

        private const string AUTHORIZATION_HEADER = "Authorization";

        internal DatabricksRestRequest() : base()
        {
            RestTimeout = TimeSpan.FromSeconds(s_defaultRestRetrySecondsTimeout);
            HttpTimeout = TimeSpan.FromSeconds(s_defaultHttpSecondsTimeout);
        }

        internal Object jsonBody { get; set; }

        internal String authorizationToken { get; set; }

        public override string ToString()
        {
            return String.Format("DatabricksRestRequest {{url: {0}, request body: {1} }}", Url.ToString(),
                jsonBody?.ToString() ?? "null");
        }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            var message = newMessage(method, Url);
            if (method != HttpMethod.Get && jsonBody != null)
            {
                var json = JsonConvert.SerializeObject(jsonBody, JsonUtils.JsonSettings);
                message.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }

            if (!string.IsNullOrEmpty(authorizationToken))
            {
                message.Headers.Add(AUTHORIZATION_HEADER, $"Bearer {authorizationToken}");
            }

            message.Headers.Accept.Add(applicationJson);
            message.Headers.UserAgent.Add(new ProductInfoHeaderValue("Databricks.Data", "1.0.0"));

            return message;
        }
    }

    // Databricks SQL API Execute Statement Request
    class ExecuteStatementRequest
    {
        [JsonProperty(PropertyName = "warehouse_id")]
        internal string WarehouseId { get; set; }

        [JsonProperty(PropertyName = "statement")]
        internal string Statement { get; set; }

        [JsonProperty(PropertyName = "wait_timeout")]
        internal string WaitTimeout { get; set; }

        [JsonProperty(PropertyName = "on_wait_timeout")]
        internal string OnWaitTimeout { get; set; }

        [JsonProperty(PropertyName = "byte_limit")]
        internal long? ByteLimit { get; set; }

        [JsonProperty(PropertyName = "catalog")]
        internal string Catalog { get; set; }

        [JsonProperty(PropertyName = "schema")]
        internal string Schema { get; set; }

        [JsonProperty(PropertyName = "disposition")]
        internal string Disposition { get; set; }

        [JsonProperty(PropertyName = "format")]
        internal string Format { get; set; }
    }

    // Databricks SQL API Get Statement Request
    class GetStatementRequest
    {
        // No body needed for GET request
    }

    // Databricks SQL API Cancel Statement Request
    class CancelStatementRequest
    {
        // No body needed for POST request
    }
}

