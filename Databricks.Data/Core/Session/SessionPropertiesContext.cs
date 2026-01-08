using System.Security;
using Databricks.Data.Core.Session;

namespace Databricks.Data.Core.Session
{
    internal class SessionPropertiesContext
    {
        public SecureString Token { get; set; }

        internal void FillSecrets(DatabricksSessionProperties properties)
        {
            // If token is provided as SecureString, use it instead of connection string token
            if (Token != null && Token.Length > 0)
            {
                // Convert SecureString to string for internal use
                // In production, you'd want to keep this secure
                var tokenValue = SecureStringHelper.ConvertToString(Token);
                if (properties.ContainsKey(DatabricksSessionProperty.TOKEN))
                {
                    properties[DatabricksSessionProperty.TOKEN] = tokenValue;
                }
                else
                {
                    properties.Add(DatabricksSessionProperty.TOKEN, tokenValue);
                }
            }
        }
    }

    internal static class SecureStringHelper
    {
        internal static string ConvertToString(SecureString secureString)
        {
            if (secureString == null || secureString.Length == 0)
                return string.Empty;

            IntPtr ptr = System.Runtime.InteropServices.Marshal.SecureStringToBSTR(secureString);
            try
            {
                return System.Runtime.InteropServices.Marshal.PtrToStringBSTR(ptr);
            }
            finally
            {
                System.Runtime.InteropServices.Marshal.ZeroFreeBSTR(ptr);
            }
        }
    }
}

