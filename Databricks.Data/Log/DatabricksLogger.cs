using System;

namespace Databricks.Data.Log
{
    public interface IDatabricksLogger
    {
        void Debug(string message);
        void Info(string message);
        void Warn(string message);
        void Error(string message);
        void Error(string message, Exception ex);
    }

    public class DatabricksLogger : IDatabricksLogger
    {
        private readonly string _name;

        public DatabricksLogger(string name)
        {
            _name = name;
        }

        public void Debug(string message)
        {
            System.Diagnostics.Debug.WriteLine($"[DEBUG] [{_name}] {message}");
        }

        public void Info(string message)
        {
            System.Diagnostics.Debug.WriteLine($"[INFO] [{_name}] {message}");
        }

        public void Warn(string message)
        {
            System.Diagnostics.Debug.WriteLine($"[WARN] [{_name}] {message}");
        }

        public void Error(string message)
        {
            System.Diagnostics.Debug.WriteLine($"[ERROR] [{_name}] {message}");
        }

        public void Error(string message, Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"[ERROR] [{_name}] {message}: {ex}");
        }
    }

    public static class DatabricksLoggerFactory
    {
        public static IDatabricksLogger GetLogger<T>()
        {
            return new DatabricksLogger(typeof(T).Name);
        }

        public static IDatabricksLogger GetLogger(string name)
        {
            return new DatabricksLogger(name);
        }
    }
}

