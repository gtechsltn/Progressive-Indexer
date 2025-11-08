## Technical
+ System.Data.SqlClient
+ System.Data.SQLite.Core v1.0.118.0

## Template
https://github.com/gtechsltn/ConsoleApp

## Project File
```
<PropertyGroup>
    <LangVersion>preview</LangVersion>
</PropertyGroup>
```

## Log4Net
```
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <system.diagnostics>
    <trace autoflush="true">
      <listeners>
        <add name="textWriterTraceListener" type="System.Diagnostics.TextWriterTraceListener" initializeData="logs\log4net.trace.txt" />
      </listeners>
    </trace>
  </system.diagnostics>
  <log4net>
    <appender name="ConsoleAppender" type="log4net.Appender.ConsoleAppender">
      <layout type="log4net.Layout.PatternLayout">
        <conversionPattern value="%date [%thread] %-5level %logger [%ndc] &lt;%property{auth}&gt; - %message%newline" />
      </layout>
    </appender>
    <appender name="DailyRollingFileAppender" type="log4net.Appender.RollingFileAppender">
      <encoding value="utf-8" />
      <threshold value="ALL" />
      <file value="logs\traceroll.day.log" />
      <appendToFile value="true" />
      <rollingStyle value="Composite" />
      <datePattern value="yyyyMMdd" />
      <maximumFileSize value="10MB" />
      <maxSizeRollBackups value="-1" />
      <CountDirection value="1" />
      <preserveLogFileNameExtension value="true" />
      <layout type="log4net.Layout.PatternLayout">
        <conversionPattern value="[${COMPUTERNAME}] %d{ISO8601} %6r %-5p [%t] %c{2}.%M() - %m%n" />
      </layout>
    </appender>
    <root>
      <!-- ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF -->
      <level value="ALL" />
      <appender-ref ref="ConsoleAppender" />
      <appender-ref ref="DailyRollingFileAppender" />
    </root>
  </log4net>
</configuration>
```

## Program.cs
```
class Program
{
    static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

    static void Main(string[] args)
    {
        AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
        
        Console.InputEncoding = Encoding.UTF8;
        Console.OutputEncoding = Encoding.UTF8;
        ...
        Environment.Exit(0);
    }

    static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        Exception ex = (Exception)e.ExceptionObject;
        Console.WriteLine("\n\n***************************************************");
        Console.WriteLine("           UNHANDLED EXCEPTION OCCURRED           ");
        Console.WriteLine("***************************************************");
        Console.WriteLine($"Error Message: {ex.Message}");
        Console.WriteLine($"Source: {ex.Source}");
        Console.WriteLine($"Stack Trace:\n{ex.StackTrace}");
    
        if (ex.InnerException != null)
        {
            Console.WriteLine("\nInner Exception:");
            Console.WriteLine($"Error Message: {ex.InnerException.Message}");
            Console.WriteLine($"Stack Trace:\n{ex.InnerException.StackTrace}");
        }

        Console.WriteLine("\nApplication will now terminate.");
        Environment.Exit(1);
    }
}
```