using System;
using System.Data;
using System.Data.Common;

namespace Databricks.Data.Client
{
    public class DatabricksDbCommandBuilder : DbCommandBuilder
    {
        protected override string GetParameterName(int parameterOrdinal)
        {
            return $"@p{parameterOrdinal}";
        }

        protected override string GetParameterName(string parameterName)
        {
            if (string.IsNullOrEmpty(parameterName))
                throw new ArgumentException("Parameter name cannot be null or empty", nameof(parameterName));
            
            if (!parameterName.StartsWith("@"))
                return "@" + parameterName;
            
            return parameterName;
        }

        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return $"@p{parameterOrdinal}";
        }

        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
            // Not implemented - Databricks uses standard parameter binding
        }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            // Not implemented - Databricks doesn't support automatic update commands
        }
    }
}

