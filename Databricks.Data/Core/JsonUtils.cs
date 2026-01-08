using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Databricks.Data.Core
{
    internal static class JsonUtils
    {
        internal static JsonSerializerSettings JsonSettings = new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore,
            ContractResolver = new DefaultContractResolver
            {
                NamingStrategy = new CamelCaseNamingStrategy()
            }
        };
    }
}

