using System;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using Databricks.Data.Core;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Log;

namespace Databricks.Data.Client
{
    public class DatabricksDbDataReader : DbDataReader
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksDbDataReader>();

        private DatabricksDbCommand dbCommand;

        private DatabricksBaseResultSet resultSet;

        private bool isClosed;

        private DataTable SchemaTable;

        private int RecordsAffectedInternal;

        internal DatabricksDbDataReader(DatabricksDbCommand command, DatabricksBaseResultSet resultSet)
        {
            this.dbCommand = command;
            this.resultSet = resultSet;
            this.isClosed = false;
            this.SchemaTable = PopulateSchemaTable(resultSet);
            RecordsAffectedInternal = resultSet.CalculateUpdateCount();
        }

        public override object this[string name]
        {
            get
            {
                return resultSet.GetValue(GetOrdinal(name));
            }
        }

        public override object this[int ordinal]
        {
            get
            {
                return resultSet.GetValue(ordinal);
            }
        }

        public override int Depth
        {
            get
            {
                return 0;
            }
        }

        public override int FieldCount
        {
            get
            {
                return resultSet.ColumnCount;
            }
        }

        public override bool HasRows
        {
            get
            {
                return !resultSet.IsClosed && resultSet.HasRows();
            }
        }

        public override bool IsClosed
        {
            get
            {
                return this.isClosed;
            }
        }

        public override int RecordsAffected { get { return RecordsAffectedInternal; } }

        public override DataTable GetSchemaTable()
        {
            return this.SchemaTable;
        }

        public string GetStatementId()
        {
            return resultSet.StatementId;
        }

        public override bool Read()
        {
            if (isClosed)
            {
                throw new DatabricksDbException(DatabricksError.DATA_READER_ALREADY_CLOSED);
            }
            return resultSet.Next();
        }

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (isClosed)
            {
                throw new DatabricksDbException(DatabricksError.DATA_READER_ALREADY_CLOSED);
            }
            return await resultSet.NextAsync().ConfigureAwait(false);
        }

        public override bool NextResult()
        {
            return resultSet.NextResult();
        }

        public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return await resultSet.NextResultAsync(cancellationToken).ConfigureAwait(false);
        }

        public override void Close()
        {
            if (!isClosed)
            {
                resultSet.Close();
                isClosed = true;
            }
        }

        public override bool GetBoolean(int ordinal)
        {
            return resultSet.GetBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return resultSet.GetByte(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return resultSet.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            return resultSet.GetChar(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return resultSet.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return resultSet.GetDateTime(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return resultSet.GetDecimal(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return resultSet.GetDouble(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return resultSet.GetFloat(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return resultSet.GetGuid(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return resultSet.GetInt16(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return resultSet.GetInt32(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return resultSet.GetInt64(ordinal);
        }

        public override string GetString(int ordinal)
        {
            return resultSet.GetString(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return resultSet.GetValue(ordinal);
        }

        public override int GetValues(object[] values)
        {
            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            return resultSet.IsDBNull(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            if (resultSet.MetaData == null)
            {
                throw new InvalidOperationException("Metadata not available");
            }

            for (int i = 0; i < resultSet.MetaData.ColumnCount; i++)
            {
                if (string.Equals(resultSet.MetaData.GetColumnName(i), name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            throw new IndexOutOfRangeException($"Column '{name}' not found");
        }

        public override string GetName(int ordinal)
        {
            if (resultSet.MetaData == null)
            {
                throw new InvalidOperationException("Metadata not available");
            }
            return resultSet.MetaData.GetColumnName(ordinal);
        }

        public override string GetDataTypeName(int ordinal)
        {
            if (resultSet.MetaData == null)
            {
                throw new InvalidOperationException("Metadata not available");
            }
            return resultSet.MetaData.GetColumnTypeName(ordinal);
        }

        public override Type GetFieldType(int ordinal)
        {
            var typeName = GetDataTypeName(ordinal);
            // Map Databricks types to .NET types
            switch (typeName.ToUpper())
            {
                case "STRING":
                case "VARCHAR":
                    return typeof(string);
                case "INT":
                case "INTEGER":
                    return typeof(int);
                case "BIGINT":
                    return typeof(long);
                case "DOUBLE":
                    return typeof(double);
                case "FLOAT":
                    return typeof(float);
                case "DECIMAL":
                    return typeof(decimal);
                case "BOOLEAN":
                    return typeof(bool);
                case "DATE":
                case "TIMESTAMP":
                    return typeof(DateTime);
                default:
                    return typeof(object);
            }
        }

        private DataTable PopulateSchemaTable(DatabricksBaseResultSet resultSet)
        {
            DataTable schemaTable = new DataTable("SchemaTable");
            schemaTable.Columns.Add("ColumnName", typeof(string));
            schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
            schemaTable.Columns.Add("DataType", typeof(Type));
            schemaTable.Columns.Add("DataTypeName", typeof(string));
            schemaTable.Columns.Add("IsNullable", typeof(bool));

            if (resultSet.MetaData != null)
            {
                for (int i = 0; i < resultSet.MetaData.ColumnCount; i++)
                {
                    DataRow row = schemaTable.NewRow();
                    row["ColumnName"] = resultSet.MetaData.GetColumnName(i);
                    row["ColumnOrdinal"] = i;
                    row["DataTypeName"] = resultSet.MetaData.GetColumnTypeName(i);
                    row["DataType"] = GetFieldType(i);
                    row["IsNullable"] = resultSet.MetaData.IsNullable(i);
                    schemaTable.Rows.Add(row);
                }
            }

            return schemaTable;
        }
    }
}

