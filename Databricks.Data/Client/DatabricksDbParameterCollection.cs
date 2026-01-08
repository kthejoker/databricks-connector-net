using System.Collections.Generic;
using System.Data.Common;

namespace Databricks.Data.Client
{
    public class DatabricksDbParameterCollection : DbParameterCollection
    {
        internal List<DatabricksDbParameter> parameterList = new List<DatabricksDbParameter>();

        public override int Count => parameterList.Count;

        public override object SyncRoot => ((System.Collections.ICollection)parameterList).SyncRoot;

        public override int Add(object value)
        {
            if (value is DatabricksDbParameter param)
            {
                parameterList.Add(param);
                return parameterList.Count - 1;
            }
            throw new System.ArgumentException("Parameter must be of type DatabricksDbParameter");
        }

        public override void AddRange(System.Array values)
        {
            foreach (var value in values)
            {
                Add(value);
            }
        }

        public override void Clear()
        {
            parameterList.Clear();
        }

        public override bool Contains(object value)
        {
            return parameterList.Contains(value as DatabricksDbParameter);
        }

        public override bool Contains(string value)
        {
            return parameterList.Exists(p => p.ParameterName == value);
        }

        public override void CopyTo(System.Array array, int index)
        {
            ((System.Collections.ICollection)parameterList).CopyTo(array, index);
        }

        public override System.Collections.IEnumerator GetEnumerator()
        {
            return parameterList.GetEnumerator();
        }

        protected override DbParameter GetParameter(int index)
        {
            return parameterList[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return parameterList.Find(p => p.ParameterName == parameterName);
        }

        public override int IndexOf(object value)
        {
            return parameterList.IndexOf(value as DatabricksDbParameter);
        }

        public override int IndexOf(string parameterName)
        {
            return parameterList.FindIndex(p => p.ParameterName == parameterName);
        }

        public override void Insert(int index, object value)
        {
            if (value is DatabricksDbParameter param)
            {
                parameterList.Insert(index, param);
            }
            else
            {
                throw new System.ArgumentException("Parameter must be of type DatabricksDbParameter");
            }
        }

        public override void Remove(object value)
        {
            parameterList.Remove(value as DatabricksDbParameter);
        }

        public override void RemoveAt(int index)
        {
            parameterList.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                RemoveAt(index);
            }
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            parameterList[index] = value as DatabricksDbParameter;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                SetParameter(index, value);
            }
        }
    }
}

