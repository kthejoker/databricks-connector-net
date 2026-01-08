using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Client;
using Databricks.Data.Log;

namespace Databricks.Data.Core
{
    internal class DatabricksResultSet : DatabricksBaseResultSet
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger<DatabricksResultSet>();

        private List<List<object>> _data;
        private int _currentRowIndex = -1;
        private int _currentChunkIndex = 0;
        private readonly DatabricksStatement _statement;
        private readonly CancellationToken _cancellationToken;
        private StatementManifest _manifest;
        private bool _hasMoreChunks = false;

        internal DatabricksResultSet(ExecuteStatementResponse response, DatabricksStatement statement, CancellationToken cancellationToken)
        {
            Statement = statement;
            _statement = statement;
            _cancellationToken = cancellationToken;
            StatementId = response.StatementId;
            _manifest = response.Manifest;
            InitializeFromManifest();
        }

        internal DatabricksResultSet(GetStatementResponse response, DatabricksStatement statement, CancellationToken cancellationToken)
        {
            Statement = statement;
            _statement = statement;
            _cancellationToken = cancellationToken;
            StatementId = response.StatementId;
            if (response.Manifest != null)
            {
                _manifest = response.Manifest;
            }
            else if (response.Result != null)
            {
                _manifest = new StatementManifest 
                { 
                    Chunks = new List<StatementChunk> 
                    { 
                        new StatementChunk 
                        { 
                            DataArray = response.Result.DataArray 
                        } 
                    } 
                };
            }
            else
            {
                _manifest = null;
            }
            InitializeFromManifest();
        }

        private void InitializeFromManifest()
        {
            if (_manifest?.Schema?.Columns != null)
            {
                ColumnCount = _manifest.Schema.Columns.Count;
                MetaData = new DatabricksResultSetMetaData(_manifest.Schema.Columns);
            }
            else
            {
                ColumnCount = 0;
                MetaData = null;
            }

            if (_manifest?.Chunks != null && _manifest.Chunks.Count > 0)
            {
                _data = _manifest.Chunks[0].DataArray ?? new List<List<object>>();
                _hasMoreChunks = _manifest.Chunks[0].NextChunkIndex.HasValue;
            }
            else
            {
                _data = new List<List<object>>();
            }
        }

        internal override bool Next()
        {
            return NextAsync().Result;
        }

        internal override async Task<bool> NextAsync()
        {
            ThrowIfClosed();

            _currentRowIndex++;

            // If we've exhausted current chunk and there are more chunks, fetch next chunk
            if (_currentRowIndex >= _data.Count && _hasMoreChunks)
            {
                await LoadNextChunk();
            }

            return _currentRowIndex < _data.Count;
        }

        private async Task LoadNextChunk()
        {
            // This would need to fetch the next chunk using the next_chunk_internal_link
            // For now, we'll mark as no more chunks
            _hasMoreChunks = false;
            await Task.CompletedTask;
        }

        internal override bool NextResult()
        {
            return false; // Databricks SQL API returns single result sets
        }

        internal override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        internal override bool HasRows()
        {
            return _data != null && _data.Count > 0;
        }

        internal override bool IsDBNull(int ordinal)
        {
            ThrowIfClosed();
            ThrowIfOutOfBounds(ordinal);

            if (_currentRowIndex < 0 || _currentRowIndex >= _data.Count)
                return true;

            var value = _data[_currentRowIndex][ordinal];
            return value == null || value == DBNull.Value;
        }

        internal override object GetValue(int ordinal)
        {
            ThrowIfClosed();
            ThrowIfOutOfBounds(ordinal);

            if (_currentRowIndex < 0 || _currentRowIndex >= _data.Count)
                throw new DatabricksDbException(DatabricksError.COLUMN_INDEX_OUT_OF_BOUND, ordinal);

            var value = _data[_currentRowIndex][ordinal];
            return value ?? DBNull.Value;
        }

        internal override bool GetBoolean(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToBoolean(value);
        }

        internal override byte GetByte(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToByte(value);
        }

        internal override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        internal override char GetChar(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToChar(value);
        }

        internal override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        internal override DateTime GetDateTime(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToDateTime(value);
        }

        internal override TimeSpan GetTimeSpan(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            if (value is TimeSpan ts) return ts;
            return TimeSpan.Parse(value.ToString());
        }

        internal override decimal GetDecimal(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToDecimal(value);
        }

        internal override double GetDouble(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToDouble(value);
        }

        internal override float GetFloat(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToSingle(value);
        }

        internal override Guid GetGuid(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            if (value is Guid guid) return guid;
            return new Guid(value.ToString());
        }

        internal override short GetInt16(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToInt16(value);
        }

        internal override int GetInt32(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToInt32(value);
        }

        internal override long GetInt64(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) throw new InvalidCastException();
            return Convert.ToInt64(value);
        }

        internal override string GetString(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value == DBNull.Value) return null;
            return value?.ToString();
        }

        internal override int CalculateUpdateCount()
        {
            // For DML statements, return affected row count
            // For now, return -1 to indicate unknown
            return -1;
        }
    }

    internal class DatabricksAsyncResultSet : DatabricksBaseResultSet
    {
        private readonly string _statementId;
        private readonly DatabricksStatement _statement;

        internal DatabricksAsyncResultSet(string statementId, DatabricksStatement statement)
        {
            _statementId = statementId;
            _statement = statement;
            StatementId = statementId;
        }

        internal override bool Next() => throw new NotSupportedException("Use async methods for async result sets");
        internal override Task<bool> NextAsync() => throw new NotSupportedException("Use GetResultsFromStatementIdAsync");
        internal override bool NextResult() => false;
        internal override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        internal override bool HasRows() => false;
        internal override bool IsDBNull(int ordinal) => throw new NotSupportedException();
        internal override object GetValue(int ordinal) => throw new NotSupportedException();
        internal override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        internal override byte GetByte(int ordinal) => throw new NotSupportedException();
        internal override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotSupportedException();
        internal override char GetChar(int ordinal) => throw new NotSupportedException();
        internal override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotSupportedException();
        internal override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        internal override TimeSpan GetTimeSpan(int ordinal) => throw new NotSupportedException();
        internal override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        internal override double GetDouble(int ordinal) => throw new NotSupportedException();
        internal override float GetFloat(int ordinal) => throw new NotSupportedException();
        internal override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        internal override short GetInt16(int ordinal) => throw new NotSupportedException();
        internal override int GetInt32(int ordinal) => throw new NotSupportedException();
        internal override long GetInt64(int ordinal) => throw new NotSupportedException();
        internal override string GetString(int ordinal) => throw new NotSupportedException();
        internal override int CalculateUpdateCount() => -1;
    }
}

