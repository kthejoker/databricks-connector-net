using System;
using System.Data;
using System.Data.Common;

namespace Databricks.Data.Client
{
    public class DatabricksDbParameter : DbParameter
    {
        private object _value;
        private string _parameterName;
        private ParameterDirection _direction = ParameterDirection.Input;
        private bool _isNullable = true;
        private int _size = 0;
        private string _sourceColumn;
        private DataRowVersion _sourceVersion = DataRowVersion.Current;

        public override DbType DbType { get; set; } = DbType.String;

        public override ParameterDirection Direction
        {
            get => _direction;
            set => _direction = value;
        }

        public override bool IsNullable
        {
            get => _isNullable;
            set => _isNullable = value;
        }

        public override string ParameterName
        {
            get => _parameterName ?? string.Empty;
            set => _parameterName = value;
        }

        public override int Size
        {
            get => _size;
            set => _size = value;
        }

        public override string SourceColumn
        {
            get => _sourceColumn ?? string.Empty;
            set => _sourceColumn = value;
        }

        public override bool SourceColumnNullMapping { get; set; }

        public override DataRowVersion SourceVersion
        {
            get => _sourceVersion;
            set => _sourceVersion = value;
        }

        public override object Value
        {
            get => _value ?? DBNull.Value;
            set => _value = value;
        }

        public override void ResetDbType()
        {
            DbType = DbType.String;
        }
    }
}

