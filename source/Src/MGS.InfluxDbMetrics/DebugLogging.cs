using System;
using System.Configuration;
using System.IO;

namespace MGS.InfluxDbMetrics
{
  public class DebugLogging
  {
    private static readonly bool LoggingEnabled = Convert.ToBoolean(ConfigurationManager.AppSettings["Logging.Enabled"]);

    public static void Log(string message)
    {
      if (LoggingEnabled)
      {
        File.AppendAllText(@"c:\mgslog\InfluxDb.log", $"{DateTime.Now}\t{message}{Environment.NewLine}");
      }
    }
  }
}
