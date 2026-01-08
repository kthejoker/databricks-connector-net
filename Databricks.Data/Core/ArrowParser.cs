using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Databricks.Data.Client;
using Databricks.Data.Log;

namespace Databricks.Data.Core
{
    /// <summary>
    /// Utility class for parsing Apache Arrow format data into ADO.NET compatible format
    /// </summary>
    internal static class ArrowParser
    {
        private static readonly IDatabricksLogger logger = DatabricksLoggerFactory.GetLogger("ArrowParser");

        /// <summary>
        /// Parses Arrow stream data from a byte array into a list of rows (List<List<object>>)
        /// </summary>
        /// <param name="arrowData">The Arrow format binary data</param>
        /// <returns>List of rows, where each row is a list of column values</returns>
        public static List<List<object>> ParseArrowStream(byte[] arrowData)
        {
            if (arrowData == null || arrowData.Length == 0)
            {
                return new List<List<object>>();
            }

            var rows = new List<List<object>>();

            try
            {
                using (var stream = new MemoryStream(arrowData))
                using (var reader = new ArrowStreamReader(stream))
                {
                    RecordBatch batch;
                    while ((batch = reader.ReadNextRecordBatch()) != null)
                    {
                        var batchRows = ParseRecordBatch(batch);
                        rows.AddRange(batchRows);
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error($"Error parsing Arrow stream: {e.Message}", e);
                throw new DatabricksDbException(
                    DatabricksError.INTERNAL_ERROR,
                    $"Failed to parse Arrow format data: {e.Message}");
            }

            return rows;
        }

        /// <summary>
        /// Parses Arrow stream data from a stream into a list of rows
        /// </summary>
        /// <param name="stream">The stream containing Arrow format data</param>
        /// <returns>List of rows, where each row is a list of column values</returns>
        public static async Task<List<List<object>>> ParseArrowStreamAsync(Stream stream)
        {
            if (stream == null || stream.Length == 0)
            {
                return new List<List<object>>();
            }

            var rows = new List<List<object>>();

            try
            {
                using (var reader = new ArrowStreamReader(stream))
                {
                    RecordBatch batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
                    {
                        var batchRows = ParseRecordBatch(batch);
                        rows.AddRange(batchRows);
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error($"Error parsing Arrow stream: {e.Message}", e);
                throw new DatabricksDbException(
                    DatabricksError.INTERNAL_ERROR,
                    $"Failed to parse Arrow format data: {e.Message}");
            }

            return rows;
        }

        /// <summary>
        /// Parses a single RecordBatch into rows
        /// </summary>
        private static List<List<object>> ParseRecordBatch(RecordBatch batch)
        {
            var rows = new List<List<object>>();
            var rowCount = batch.Length;
            var columnCount = batch.ColumnCount;

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                var row = new List<object>();
                for (int colIndex = 0; colIndex < columnCount; colIndex++)
                {
                    var array = batch.Column(colIndex);
                    var value = GetValueFromArrowArray(array, rowIndex);
                    row.Add(value);
                }
                rows.Add(row);
            }

            return rows;
        }

        /// <summary>
        /// Extracts a value from an Arrow array at the specified index
        /// </summary>
        private static object GetValueFromArrowArray(IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                return DBNull.Value;
            }

            // Handle different Arrow array types
            switch (array)
            {
                case BooleanArray boolArray:
                    return boolArray.GetValue(index).GetValueOrDefault();

                case Int8Array int8Array:
                    return int8Array.GetValue(index).GetValueOrDefault();

                case Int16Array int16Array:
                    return int16Array.GetValue(index).GetValueOrDefault();

                case Int32Array int32Array:
                    return int32Array.GetValue(index).GetValueOrDefault();

                case Int64Array int64Array:
                    return int64Array.GetValue(index).GetValueOrDefault();

                case UInt8Array uint8Array:
                    return uint8Array.GetValue(index).GetValueOrDefault();

                case UInt16Array uint16Array:
                    return uint16Array.GetValue(index).GetValueOrDefault();

                case UInt32Array uint32Array:
                    return uint32Array.GetValue(index).GetValueOrDefault();

                case UInt64Array uint64Array:
                    return uint64Array.GetValue(index).GetValueOrDefault();

                case FloatArray floatArray:
                    return floatArray.GetValue(index).GetValueOrDefault();

                case DoubleArray doubleArray:
                    return doubleArray.GetValue(index).GetValueOrDefault();

                case Decimal128Array decimalArray:
                    return decimalArray.GetValue(index);

                case Date32Array date32Array:
                    return date32Array.GetDateTime(index);

                case Date64Array date64Array:
                    return date64Array.GetDateTime(index);

                case TimestampArray timestampArray:
                    return timestampArray.GetTimestamp(index);

                case Time32Array time32Array:
                    // Time32Array stores time in seconds
                    return TimeSpan.FromSeconds(time32Array.GetValue(index).GetValueOrDefault());

                case Time64Array time64Array:
                    // Time64Array stores time in microseconds
                    var microseconds = time64Array.GetValue(index).GetValueOrDefault();
                    return TimeSpan.FromTicks(microseconds / 10); // Convert microseconds to ticks (1 tick = 100 nanoseconds = 0.1 microseconds)

                case StringArray stringArray:
                    return stringArray.GetString(index);

                case BinaryArray binaryArray:
                    return binaryArray.GetBytes(index).ToArray();

                default:
                    // For unsupported types, log a warning and return null
                    logger.Warn($"Unsupported Arrow array type: {array.GetType().Name}, returning DBNull");
                    return DBNull.Value;
            }
        }
    }
}

