# How to build and run a system that fail-fast + progressive indexing + checkpoint JSON + SQLite + CSV
+ [D:\gtechsltn\Progressive-Indexer](https://github.com/gtechsltn/Progressive-Indexer)
+ Open Visual Studio 2026
+ Ctrl + O to open .\src\ProgressiveIndexerService.slnx
+ Build solution
+ Startup project: ProgressiveIndexerDemo.csproj (Program.cs)
+ Main project: ProgressiveIndexerService.csproj (Program.cs)
+ Change App.config of 2 above projects
```
<appSettings>
	<add key="ErrorSimulationFlag" value="1" />
	<add key="ErrorSimulationJobId" value="17" />		
	<add key="ErrorSimulationRecordId" value="576213" />
</appSettings>
```
+ F5 to run
+ [ChatGPT](https://chatgpt.com/c/690ede44-aa20-8323-9461-ff85f7c4a9d5)

# Success
```
Completed with Success! JobId = 1.
DONE! JobId = 1.
```

# Failure
```
Completed with Failure. JobId = 1.
```

# Bản yêu cầu cụ thể đầy đủ nhất

* Nếu có lỗi ngoài dự kiến (critical error) xảy ra, chương trình phải:

* Dừng ngay toàn bộ Job / ứng dụng.

* Cập nhật trạng thái hiện tại vào tệp JSON (Job-level + Record-level).

* Cập nhật vào SQLite, đánh dấu bản ghi lỗi hoặc ghi nhận trạng thái chưa hoàn tất.

* Hãy viết lại phiên bản Windows Service progressive indexing với cơ chế "fail-fast" nhưng vẫn lưu checkpoint để có thể resume hoặc báo lỗi chính xác.

## Dưới đây là phiên bản toàn bộ Program.cs hoàn chỉnh, tích hợp:

* Job-level + Record-level progressive indexing

* Ghi dữ liệu vào SQLite ngay sau khi xử lý mỗi record

* Fail-fast: nếu xảy ra lỗi critical, chương trình dừng ngay

* Cập nhật checkpoint (JSON + SQLite) trước khi dừng

* Xuất CSV từ SQLite khi hoàn tất hoặc khi gặp lỗi

* Lưu danh sách OID lỗi để tham khảo

# Bản updated version 6

* Các điểm nổi bật:

* Progressive indexing ở cả Job-level và Record-level.

* Ghi SQLite ngay sau khi xử lý mỗi record.

* Danh sách OID lỗi được lưu trong JobIndexer.FailedOids.

* CSV cuối cùng xuất từ SQLite, không lưu toàn bộ dữ liệu trong CSV trực tiếp.

* Error handling toàn bộ ứng dụng, kể cả lỗi không mong muốn và lỗi bất đồng bộ (TaskScheduler_UnobservedTaskException).

# Bản updated version 5

* Mình đã cập nhật toàn bộ code của bạn theo yêu cầu mới, gồm:

* Job-level + Record-level progressive indexing

* Ghi dữ liệu từng record vào SQLite ngay sau khi xử lý

* Bắt lỗi từng record, tiếp tục record khác nếu có lỗi

* Checkpoint JSON cập nhật liên tục

* CSV cuối cùng xuất từ DB

* Bắt lỗi toàn bộ ứng dụng (unhandled + unobserved tasks)

* Record OID dùng long

# Bản Windows Service (ProgressiveIndexerService)

Dưới đây là tính năng của bản Windows Service progressive indexing hoàn chỉnh với các cập nhật:

* JobID = int

* RecordID / OID = long

* Job-level + Record-level progressive indexing

* Ghi dữ liệu từng record ngay vào SQLite (BIGINT OID)

* Xử lý lỗi từng record, tiếp tục khi xảy ra lỗi

* Checkpoint JSON cho Job-level + Record-level

* CSV cuối cùng xuất từ DB

* Bắt lỗi toàn cục cho toàn ứng dụng

* DB chung ở thư mục AppDomain.CurrentDomain.BaseDirectory\DB\ThirdSight.db

# Features (Tính năng)

## Tính năng phiên bản mới (updated)

* Record-level progressive indexing: mỗi OID lưu trong thư mục riêng .OID.

* Job-level progressive indexing: lưu vào ExportJob<JobID>-fullindex.json + ExportJob<JobID>-indexing-status.json.

* Kết quả lưu vào SQLite ngay sau khi xử lý record.

* Cuối cùng xuất CSV tổng hợp từ SQLite, đảm bảo không mất dữ liệu khi Job bị dừng giữa chừng.

* Tự mở rộng bitArray khi số lượng OID tăng.

## Job-level progressive indexing:

* Lưu tiến trình toàn bộ Job vào ExportJob<JobID>-fullindex.json + ExportJob<JobID>-indexing-status.json.

* Khi chạy lại, chỉ xử lý OID chưa export.

## Record-level progressive indexing:

* Mỗi OID có thư mục .OID với fullindex + indexing-status.

* Quản lý tiến trình OID riêng biệt, có thể restart từng OID.

* Cập nhật Job-level bitArray: sau khi xử lý xong mỗi OID.

* CSV tổng hợp Job: Job<JobID>-data.csv chứa tất cả record của Job.

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

## Cách chạy

```
D:
cd D:\gtechsltn\Progressive-Indexer\src\ProgressiveIndexerDemo\ProgressiveIndexerDemo\bin\Debug
ProgressiveIndexerDemo 7 576210,576211,576212,576213,576214,576215
```

## Khuyễn nghị dùng đường dẫn thư mục hiện tại

### Environment.CurrentDirectory

* Là thư mục làm việc hiện tại của tiến trình (working directory).

* Có thể thay đổi trong quá trình chạy bằng Directory.SetCurrentDirectory().

* Phụ thuộc vào cách chạy ứng dụng:

* Chạy từ cmd: là thư mục hiện tại của command prompt

* Chạy từ Visual Studio: thường là thư mục dự án bin\Debug\netX

* Ví dụ rủi ro:

```
Console.WriteLine(Environment.CurrentDirectory);
// Khi chạy từ shortcut, có thể là C:\Users\Username\Desktop
```

### AppDomain.CurrentDomain.BaseDirectory

* Là thư mục chứa file exe hoặc dll của ứng dụng.

* Không thay đổi trong suốt thời gian chạy.

* Thích hợp để lưu dữ liệu cố định hoặc file cấu hình đi kèm app, ví dụ DB hoặc file log.

Ví dụ:

```
Console.WriteLine(AppDomain.CurrentDomain.BaseDirectory);
// C:\Projects\MyApp\bin\Debug\net7.0\
```

### Khuyến nghị cho DB

* Vì DB phải cố định và không phụ thuộc vào nơi chạy:
* Dùng AppDomain.CurrentDomain.BaseDirectory sẽ an toàn hơn.
* Nếu dùng Environment.CurrentDirectory, DB có thể bị tạo nhầm chỗ nếu chạy từ command prompt hay task scheduler.

# Kỹ thuật sửa dụng
```
<PropertyGroup>
  <LangVersion>preview</LangVersion>
</PropertyGroup>
```

```
ProgressiveIndexerService 8 576210,576211,576212,576213,576214,576215,576216,576217,576218,576219,576220,576221,576222,576223,576224,576225,576226,576227,576228,576229,576230,576231,576232,576233,576234,576235,576236,576237,576238,576239,576240,576241,576242,576243,576244,576245,576246,576247,576248,576249,576250,576251,576252,576253,576254,576255,576256,576257,576258,576259,576260,576261,576262,576263,576264,576265,576266,576267,576268,576269,576270,576271,576272,576273,576274,576275,576276,576277,576278,576279,576280,576281,576282,576283,576284,576285,576286,576287,576288,576289,576290,576291,576292,576293,576294,576295,576296,576297,576298,576299,576300,576301,576302,576303,576304,576305,576306,576307,576308,576309
```
