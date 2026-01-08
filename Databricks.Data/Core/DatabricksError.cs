using System;

namespace Databricks.Data.Core
{
    public enum DatabricksError
    {
        [DatabricksErrorAttr(errorCode = 270001)]
        INTERNAL_ERROR,

        [DatabricksErrorAttr(errorCode = 270002)]
        COLUMN_INDEX_OUT_OF_BOUND,

        [DatabricksErrorAttr(errorCode = 270003)]
        INVALID_DATA_CONVERSION,

        [DatabricksErrorAttr(errorCode = 270004)]
        STATEMENT_ALREADY_RUNNING_QUERY,

        [DatabricksErrorAttr(errorCode = 270005)]
        QUERY_CANCELLED,

        [DatabricksErrorAttr(errorCode = 270006)]
        MISSING_CONNECTION_PROPERTY,

        [DatabricksErrorAttr(errorCode = 270007)]
        REQUEST_TIMEOUT,

        [DatabricksErrorAttr(errorCode = 270008)]
        INVALID_CONNECTION_STRING,

        [DatabricksErrorAttr(errorCode = 270009)]
        UNSUPPORTED_FEATURE,

        [DatabricksErrorAttr(errorCode = 270010)]
        DATA_READER_ALREADY_CLOSED,

        [DatabricksErrorAttr(errorCode = 270011)]
        UNKNOWN_AUTHENTICATOR,

        [DatabricksErrorAttr(errorCode = 270012)]
        UNSUPPORTED_PLATFORM,

        [DatabricksErrorAttr(errorCode = 270013)]
        AUTHENTICATION_FAILED,

        [DatabricksErrorAttr(errorCode = 270014)]
        INVALID_TOKEN,

        [DatabricksErrorAttr(errorCode = 270015)]
        EXECUTE_COMMAND_ON_CLOSED_CONNECTION,

        [DatabricksErrorAttr(errorCode = 270016)]
        INVALID_CONNECTION_PARAMETER_VALUE
    }

    class DatabricksErrorAttr : Attribute
    {
        public int errorCode { get; set; }
    }

    public static class ErrorExtensions
    {
        public static T GetAttribute<T>(this Enum enumVal) where T : Attribute
        {
            var type = enumVal.GetType();
            var memInfo = type.GetMember(enumVal.ToString());
            var attributes = memInfo[0].GetCustomAttributes(typeof(T), false);
            return (attributes.Length > 0) ? (T)attributes[0] : null;
        }
    }
}

