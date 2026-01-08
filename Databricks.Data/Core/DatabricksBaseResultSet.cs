using System;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Client;

namespace Databricks.Data.Core
{
    abstract class DatabricksBaseResultSet
    {
        internal DatabricksStatement Statement;

        internal DatabricksResultSetMetaData MetaData { get; set; }

        internal int ColumnCount;

        internal bool IsClosed;

        internal string StatementId;

        internal abstract bool Next();

        internal abstract Task<bool> NextAsync();

        internal abstract bool NextResult();

        internal abstract Task<bool> NextResultAsync(CancellationToken cancellationToken);

        internal abstract bool HasRows();

        protected DatabricksBaseResultSet()
        {
        }

        internal abstract bool IsDBNull(int ordinal);

        internal abstract object GetValue(int ordinal);

        internal abstract bool GetBoolean(int ordinal);

        internal abstract byte GetByte(int ordinal);

        internal abstract long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length);

        internal abstract char GetChar(int ordinal);

        internal abstract long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length);

        internal abstract DateTime GetDateTime(int ordinal);

        internal abstract TimeSpan GetTimeSpan(int ordinal);

        internal abstract decimal GetDecimal(int ordinal);

        internal abstract double GetDouble(int ordinal);

        internal abstract float GetFloat(int ordinal);

        internal abstract Guid GetGuid(int ordinal);

        internal abstract short GetInt16(int ordinal);

        internal abstract int GetInt32(int ordinal);

        internal abstract long GetInt64(int ordinal);

        internal abstract string GetString(int ordinal);

        internal void Close()
        {
            IsClosed = true;
        }

        internal void ThrowIfClosed()
        {
            if (IsClosed)
                throw new DatabricksDbException(DatabricksError.DATA_READER_ALREADY_CLOSED);
        }

        internal void ThrowIfOutOfBounds(int ordinal)
        {
            if (ordinal < 0 || ordinal >= ColumnCount)
                throw new DatabricksDbException(DatabricksError.COLUMN_INDEX_OUT_OF_BOUND, ordinal);
        }

        internal abstract int CalculateUpdateCount();
    }
}

