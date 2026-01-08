using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Databricks.Data.Client;
using Databricks.Data.Log;
using System.IO;

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
        private Task _initialChunkLoadTask = null;
        private readonly object _dataLock = new object();

        internal DatabricksResultSet(ExecuteStatementResponse response, DatabricksStatement statement, CancellationToken cancellationToken)
        {
            Statement = statement;
            _statement = statement;
            _cancellationToken = cancellationToken;
            StatementId = response.StatementId;
            _manifest = response.Manifest;
            
            // If result has external_links, populate them into the manifest chunks
            if (response.Result != null && response.Result.ExternalLinks != null && response.Result.ExternalLinks.Count > 0)
            {
                // Ensure manifest has chunks
                if (_manifest?.Chunks == null || _manifest.Chunks.Count == 0)
                {
                    _manifest = _manifest ?? new StatementManifest { Chunks = new List<StatementChunk>() };
                    _manifest.Chunks = _manifest.Chunks ?? new List<StatementChunk>();
                }
                
                // Add external links to the first chunk (or create it if it doesn't exist)
                if (_manifest.Chunks.Count == 0)
                {
                    _manifest.Chunks.Add(new StatementChunk
                    {
                        ChunkIndex = 0,
                        RowCount = response.Result.RowCount,
                        RowOffset = response.Result.RowOffset,
                        ExternalLinks = response.Result.ExternalLinks
                    });
                }
                else
                {
                    _manifest.Chunks[0].ExternalLinks = response.Result.ExternalLinks;
                }
            }
            
            InitializeFromManifest();
            // For ARROW_STREAM with EXTERNAL_LINKS, start fetching the first chunk asynchronously
            StartInitialChunkLoad();
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
            // For ARROW_STREAM with EXTERNAL_LINKS, start fetching the first chunk asynchronously
            StartInitialChunkLoad();
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
                var firstChunk = _manifest.Chunks[0];
                // Handle both INLINE (data_array) and EXTERNAL_LINKS (external_links) dispositions
                if (firstChunk.DataArray != null && firstChunk.DataArray.Count > 0)
                {
                    _data = firstChunk.DataArray;
                }
                else if (firstChunk.ExternalLinks != null && firstChunk.ExternalLinks.Count > 0)
                {
                    // For EXTERNAL_LINKS, we'll need to fetch chunks as needed
                    // For now, initialize empty and fetch on demand
                    _data = new List<List<object>>();
                    logger.Debug("EXTERNAL_LINKS disposition detected - chunks will be fetched on demand");
                }
                else
                {
                    _data = new List<List<object>>();
                }
                _hasMoreChunks = firstChunk.NextChunkIndex.HasValue || !string.IsNullOrEmpty(firstChunk.NextChunkInternalLink);
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

            // Wait for initial chunk load if it's still in progress
            if (_initialChunkLoadTask != null)
            {
                await _initialChunkLoadTask.ConfigureAwait(false);
                _initialChunkLoadTask = null; // Clear after completion
            }

            _currentRowIndex++;

            // Check if we need to load more data (outside lock to avoid await in lock)
            bool needMoreData = false;
            int currentDataCount = 0;
            lock (_dataLock)
            {
                currentDataCount = _data?.Count ?? 0;
                needMoreData = _currentRowIndex >= currentDataCount && _hasMoreChunks;
            }

            // If we've exhausted current chunk and there are more chunks, fetch next chunk
            if (needMoreData)
            {
                await LoadNextChunk();
                // Re-check count after loading
                lock (_dataLock)
                {
                    currentDataCount = _data?.Count ?? 0;
                }
            }

            return _currentRowIndex < currentDataCount;
        }

        /// <summary>
        /// Starts loading the first chunk asynchronously when EXTERNAL_LINKS are detected
        /// </summary>
        private void StartInitialChunkLoad()
        {
            if (_manifest?.Chunks == null || _manifest.Chunks.Count == 0)
                return;

            var firstChunk = _manifest.Chunks[0];
            if (firstChunk.ExternalLinks == null || firstChunk.ExternalLinks.Count == 0)
                return;

            // Check if we're using ARROW_STREAM format
            bool isArrowFormat = _manifest.Format == "ARROW_STREAM" || _manifest.Format == "ARROW";
            if (!isArrowFormat)
                return;

            // Start async task to fetch and parse the first chunk
            _initialChunkLoadTask = Task.Run(async () =>
            {
                try
                {
                    logger.Debug("Starting async fetch of first chunk from external link");
                    var externalLink = firstChunk.ExternalLinks[0];
                    byte[] arrowData = await _statement.GetExternalLinkArrowDataAsync(externalLink.ExternalLinkUrl, _cancellationToken);
                    
                    if (arrowData != null && arrowData.Length > 0)
                    {
                        // Parse Arrow format data
                        var chunkData = ArrowParser.ParseArrowStream(arrowData);
                        
                        // Update _data in a thread-safe manner
                        lock (_dataLock)
                        {
                            _data = chunkData;
                        }
                        
                        logger.Debug($"Successfully loaded first chunk: {chunkData.Count} rows");
                    }
                }
                catch (Exception e)
                {
                    logger.Error($"Error loading initial chunk: {e.Message}", e);
                    // Don't throw - let Next() handle the error when it tries to read
                }
            }, _cancellationToken);
        }

        private async Task LoadNextChunk()
        {
            if (!_hasMoreChunks || _manifest?.Chunks == null || _manifest.Chunks.Count == 0)
            {
                _hasMoreChunks = false;
                return;
            }

            var currentChunk = _manifest.Chunks[_currentChunkIndex];
            if (currentChunk.NextChunkIndex == null && string.IsNullOrEmpty(currentChunk.NextChunkInternalLink))
            {
                _hasMoreChunks = false;
                return;
            }

            try
            {
                // Fetch next chunk using the internal link or chunk index
                int nextChunkIndex = currentChunk.NextChunkIndex ?? (_currentChunkIndex + 1);
                List<List<object>> chunkData = null;
                StatementChunkResponse chunkResponse = null;

                // Check if we're using ARROW_STREAM format (EXTERNAL_LINKS disposition)
                bool isArrowFormat = _manifest.Format == "ARROW_STREAM" || _manifest.Format == "ARROW";

                if (isArrowFormat)
                {
                    // For ARROW_STREAM format, fetch binary Arrow data and parse it
                    byte[] arrowData = null;

                    // First, get chunk metadata to check for external links
                    chunkResponse = await _statement.GetChunkAsync(StatementId, nextChunkIndex, _cancellationToken);
                    
                    if (chunkResponse?.ExternalLinks != null && chunkResponse.ExternalLinks.Count > 0)
                    {
                        // Fetch from external link (presigned URL)
                        var externalLink = chunkResponse.ExternalLinks[0];
                        arrowData = await _statement.GetExternalLinkArrowDataAsync(externalLink.ExternalLinkUrl, _cancellationToken);
                    }
                    else
                    {
                        // Fetch from internal chunk endpoint as Arrow binary
                        arrowData = await _statement.GetChunkArrowDataAsync(StatementId, nextChunkIndex, _cancellationToken);
                    }

                    if (arrowData != null && arrowData.Length > 0)
                    {
                        // Parse Arrow format data
                        chunkData = ArrowParser.ParseArrowStream(arrowData);
                    }
                }
                else
                {
                    // For JSON_ARRAY format, use the standard chunk endpoint
                    chunkResponse = await _statement.GetChunkAsync(StatementId, nextChunkIndex, _cancellationToken);

                    if (chunkResponse != null)
                    {
                        if (chunkResponse.DataArray != null && chunkResponse.DataArray.Count > 0)
                        {
                            // INLINE disposition: data is in data_array
                            chunkData = chunkResponse.DataArray;
                        }
                        else if (chunkResponse.ExternalLinks != null && chunkResponse.ExternalLinks.Count > 0)
                        {
                            // EXTERNAL_LINKS with JSON_ARRAY - fetch from external link
                            var externalLink = chunkResponse.ExternalLinks[0];
                            var jsonData = await _statement.GetExternalLinkArrowDataAsync(externalLink.ExternalLinkUrl, _cancellationToken);
                            // For JSON_ARRAY, the external link would contain JSON, not Arrow
                            // This would need JSON parsing - for now, log a warning
                            logger.Warn("JSON_ARRAY with EXTERNAL_LINKS not yet fully implemented");
                            chunkData = new List<List<object>>();
                        }
                    }
                }

                if (chunkData != null && chunkData.Count > 0)
                {
                    // Append new chunk data to existing data (thread-safe)
                    lock (_dataLock)
                    {
                        if (_data == null)
                        {
                            _data = new List<List<object>>();
                        }
                        _data.AddRange(chunkData);
                    }
                    _currentChunkIndex = nextChunkIndex;
                    
                    // Check if there are more chunks (use chunkResponse if we have it, otherwise fetch)
                    if (chunkResponse == null)
                    {
                        chunkResponse = await _statement.GetChunkAsync(StatementId, nextChunkIndex, _cancellationToken);
                    }
                    _hasMoreChunks = chunkResponse?.NextChunkIndex.HasValue == true || !string.IsNullOrEmpty(chunkResponse?.NextChunkInternalLink);
                }
                else
                {
                    _hasMoreChunks = false;
                }
            }
            catch (Exception e)
            {
                logger.Error($"Error loading next chunk: {e.Message}", e);
                _hasMoreChunks = false;
            }
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
            lock (_dataLock)
            {
                return _data != null && _data.Count > 0;
            }
        }

        internal override bool IsDBNull(int ordinal)
        {
            ThrowIfClosed();
            ThrowIfOutOfBounds(ordinal);

            lock (_dataLock)
            {
                if (_currentRowIndex < 0 || _currentRowIndex >= (_data?.Count ?? 0))
                    return true;

                var value = _data[_currentRowIndex][ordinal];
                return value == null || value == DBNull.Value;
            }
        }

        internal override object GetValue(int ordinal)
        {
            ThrowIfClosed();
            ThrowIfOutOfBounds(ordinal);

            lock (_dataLock)
            {
                if (_currentRowIndex < 0 || _currentRowIndex >= _data.Count)
                    throw new DatabricksDbException(DatabricksError.COLUMN_INDEX_OUT_OF_BOUND, ordinal);

                var value = _data[_currentRowIndex][ordinal];
                return value ?? DBNull.Value;
            }
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

