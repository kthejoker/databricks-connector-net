using System;
using System.Collections.Generic;
using Databricks.Data.Core;

namespace Databricks.Data.Core
{
    internal class DatabricksResultSetMetaData
    {
        private List<DatabricksColumnMetadata> _columns;

        internal DatabricksResultSetMetaData(List<StatementColumn> columns)
        {
            _columns = new List<DatabricksColumnMetadata>();
            foreach (var col in columns)
            {
                _columns.Add(new DatabricksColumnMetadata
                {
                    Name = col.Name,
                    TypeName = col.TypeName,
                    TypeText = col.TypeText,
                    Position = col.Position,
                    Nullable = col.Nullable ?? true
                });
            }
        }

        internal int ColumnCount => _columns.Count;

        internal string GetColumnName(int ordinal)
        {
            if (ordinal < 0 || ordinal >= _columns.Count)
                throw new IndexOutOfRangeException();
            return _columns[ordinal].Name;
        }

        internal string GetColumnTypeName(int ordinal)
        {
            if (ordinal < 0 || ordinal >= _columns.Count)
                throw new IndexOutOfRangeException();
            return _columns[ordinal].TypeName;
        }

        internal bool IsNullable(int ordinal)
        {
            if (ordinal < 0 || ordinal >= _columns.Count)
                throw new IndexOutOfRangeException();
            return _columns[ordinal].Nullable;
        }
    }

    internal class DatabricksColumnMetadata
    {
        internal string Name { get; set; }
        internal string TypeName { get; set; }
        internal string TypeText { get; set; }
        internal int Position { get; set; }
        internal bool Nullable { get; set; }
    }
}

