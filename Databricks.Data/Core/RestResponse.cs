using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Databricks.Data.Client;

namespace Databricks.Data.Core
{
    abstract class BaseRestResponse
    {
        [JsonProperty(PropertyName = "message", NullValueHandling = NullValueHandling.Ignore)]
        internal String Message { get; set; }

        [JsonProperty(PropertyName = "error_code", NullValueHandling = NullValueHandling.Ignore)]
        internal String ErrorCode { get; set; }

        internal void FilterFailedResponse()
        {
            if (!string.IsNullOrEmpty(ErrorCode))
            {
                DatabricksDbException e = new DatabricksDbException("", 0, Message ?? ErrorCode, "");
                throw e;
            }
        }
    }

    // Databricks SQL API Execute Statement Response
    internal class ExecuteStatementResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "statement_id")]
        internal string StatementId { get; set; }

        [JsonProperty(PropertyName = "status")]
        internal StatementStatus Status { get; set; }

        [JsonProperty(PropertyName = "manifest")]
        internal StatementManifest Manifest { get; set; }
    }

    // Databricks SQL API Statement Status
    internal class StatementStatus
    {
        [JsonProperty(PropertyName = "state")]
        internal string State { get; set; }

        [JsonProperty(PropertyName = "error", NullValueHandling = NullValueHandling.Ignore)]
        internal StatementError Error { get; set; }
    }

    // Databricks SQL API Statement Error
    internal class StatementError
    {
        [JsonProperty(PropertyName = "error_code")]
        internal string ErrorCode { get; set; }

        [JsonProperty(PropertyName = "message")]
        internal string Message { get; set; }
    }

    // Databricks SQL API Statement Manifest
    internal class StatementManifest
    {
        [JsonProperty(PropertyName = "chunks")]
        internal List<StatementChunk> Chunks { get; set; }

        [JsonProperty(PropertyName = "total_chunk_count")]
        internal int TotalChunkCount { get; set; }

        [JsonProperty(PropertyName = "format")]
        internal string Format { get; set; }

        [JsonProperty(PropertyName = "schema")]
        internal StatementSchema Schema { get; set; }
    }

    // Databricks SQL API Statement Chunk
    internal class StatementChunk
    {
        [JsonProperty(PropertyName = "chunk_index")]
        internal int ChunkIndex { get; set; }

        [JsonProperty(PropertyName = "row_count")]
        internal int RowCount { get; set; }

        [JsonProperty(PropertyName = "row_offset")]
        internal long RowOffset { get; set; }

        [JsonProperty(PropertyName = "data_array")]
        internal List<List<object>> DataArray { get; set; }

        [JsonProperty(PropertyName = "next_chunk_index", NullValueHandling = NullValueHandling.Ignore)]
        internal int? NextChunkIndex { get; set; }

        [JsonProperty(PropertyName = "next_chunk_internal_link", NullValueHandling = NullValueHandling.Ignore)]
        internal string NextChunkInternalLink { get; set; }
    }

    // Databricks SQL API Statement Schema
    internal class StatementSchema
    {
        [JsonProperty(PropertyName = "columns")]
        internal List<StatementColumn> Columns { get; set; }
    }

    // Databricks SQL API Statement Column
    internal class StatementColumn
    {
        [JsonProperty(PropertyName = "name")]
        internal string Name { get; set; }

        [JsonProperty(PropertyName = "type_name")]
        internal string TypeName { get; set; }

        [JsonProperty(PropertyName = "type_text")]
        internal string TypeText { get; set; }

        [JsonProperty(PropertyName = "type_json", NullValueHandling = NullValueHandling.Ignore)]
        internal string TypeJson { get; set; }

        [JsonProperty(PropertyName = "position")]
        internal int Position { get; set; }

        [JsonProperty(PropertyName = "nullable", NullValueHandling = NullValueHandling.Ignore)]
        internal bool? Nullable { get; set; }
    }

    // Databricks SQL API Get Statement Response
    internal class GetStatementResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "statement_id")]
        internal string StatementId { get; set; }

        [JsonProperty(PropertyName = "status")]
        internal StatementStatus Status { get; set; }

        [JsonProperty(PropertyName = "manifest")]
        internal StatementManifest Manifest { get; set; }

        [JsonProperty(PropertyName = "result")]
        internal StatementResult Result { get; set; }
    }

    // Databricks SQL API Statement Result
    internal class StatementResult
    {
        [JsonProperty(PropertyName = "chunk_index")]
        internal int ChunkIndex { get; set; }

        [JsonProperty(PropertyName = "row_count")]
        internal int RowCount { get; set; }

        [JsonProperty(PropertyName = "row_offset")]
        internal long RowOffset { get; set; }

        [JsonProperty(PropertyName = "data_array")]
        internal List<List<object>> DataArray { get; set; }

        [JsonProperty(PropertyName = "next_chunk_index", NullValueHandling = NullValueHandling.Ignore)]
        internal int? NextChunkIndex { get; set; }

        [JsonProperty(PropertyName = "next_chunk_internal_link", NullValueHandling = NullValueHandling.Ignore)]
        internal string NextChunkInternalLink { get; set; }
    }
}

