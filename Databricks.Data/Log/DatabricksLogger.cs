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
        private static bool _consoleOutputEnabled = false;

        public DatabricksLogger(string name)
        {
            _name = name;
        }

        public static void EnableConsoleOutput(bool enable = true)
        {
            _consoleOutputEnabled = enable;
        }

        private void WriteLine(string level, string message)
        {
            var logMessage = $"[{level}] [{_name}] {message}";
            System.Diagnostics.Debug.WriteLine(logMessage);
            if (_consoleOutputEnabled)
            {
                Console.WriteLine(logMessage);
            }
        }

        public void Debug(string message)
        {
            WriteLine("DEBUG", message);
        }

        public void Info(string message)
        {
            WriteLine("INFO", message);
        }

        public void Warn(string message)
        {
            WriteLine("WARN", message);
        }

        public void Error(string message)
        {
            WriteLine("ERROR", message);
        }

        public void Error(string message, Exception ex)
        {
            WriteLine("ERROR", $"{message}: {ex}");
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

