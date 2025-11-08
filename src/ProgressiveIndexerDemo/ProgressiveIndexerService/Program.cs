using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using log4net;

namespace ProgressiveIndexerService
{
    #region Data Models
    public class FullIndex
    {
        public DateTime _utcCreated { get; set; }
        public int _arrayLength { get; set; }
        public int _minOID { get; set; }
        public int _maxOID { get; set; }
        public string _bitArray { get; set; }
    }

    public class IndexingStatus
    {
        public DateTime LastIndexed { get; set; }
        public int LastOffset { get; set; }
        public bool FullIndexingCompleted { get; set; }
    }

    public class DataRecord
    {
        public int OID { get; set; }
        public string Value { get; set; }
    }
    #endregion

    #region RecordIndexer
    public class RecordIndexer
    {
        private readonly int _jobId;
        private readonly int _oid;
        private readonly string _recordDir;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupStatusFile;

        private readonly string _dbFile;

        public RecordIndexer(int jobId, int oid)
        {
            _jobId = jobId;
            _oid = oid;

            _recordDir = $@"D:\DM\PIDX\{jobId}\.{oid}";
            Directory.CreateDirectory(_recordDir);

            _fullIndexFile = Path.Combine(_recordDir, $"{oid}-fullindex.json");
            _statusFile = Path.Combine(_recordDir, $"{oid}-indexing-status.json");
            _backupStatusFile = Path.Combine(_recordDir, $"{oid}-indexing-status.json.bak");

            // DB chung: <CurrentAppDirectory>\DB\ThirdSight.db
            string dbDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DB");
            Directory.CreateDirectory(dbDir);
            _dbFile = Path.Combine(dbDir, "ThirdSight.db");

            InitializeFiles();
        }

        private void InitializeFiles()
        {
            // Tạo file JSON nếu chưa tồn tại
            if (!File.Exists(_fullIndexFile))
            {
                var fullIndex = new FullIndex
                {
                    _utcCreated = DateTime.UtcNow,
                    _arrayLength = 1,
                    _minOID = _oid,
                    _maxOID = _oid,
                    _bitArray = Convert.ToBase64String(new byte[] { 0 })
                };
                File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(fullIndex, new JsonSerializerOptions { WriteIndented = true }));

                var status = new IndexingStatus
                {
                    LastIndexed = DateTime.UtcNow,
                    LastOffset = 0,
                    FullIndexingCompleted = false
                };
                string json = JsonSerializer.Serialize(status, new JsonSerializerOptions { WriteIndented = true });
                File.WriteAllText(_statusFile, json);
                File.WriteAllText(_backupStatusFile, json);
            }

            // Tạo DB nếu chưa tồn tại
            if (!File.Exists(_dbFile))
            {
                using var conn = new SQLiteConnection($"Data Source={_dbFile}");
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = @"
                    CREATE TABLE IF NOT EXISTS Records (
                        JobID INTEGER,
                        OID INTEGER,
                        Value TEXT,
                        ProcessedAt TEXT,
                        PRIMARY KEY(JobID, OID)
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        public async Task RunAsync()
        {
            var fullIndex = JsonSerializer.Deserialize<FullIndex>(File.ReadAllText(_fullIndexFile))!;
            var status = JsonSerializer.Deserialize<IndexingStatus>(File.ReadAllText(_statusFile))!;
            byte[] bitArray = Convert.FromBase64String(fullIndex._bitArray);

            int totalBits = fullIndex._arrayLength * 8;

            using var conn = new SQLiteConnection($"Data Source={_dbFile}");
            conn.Open();

            for (int i = status.LastOffset; i < totalBits; i++)
            {
                try
                {
                    if (!IsBitSet(ref bitArray, i))
                    {
                        int currentOID = fullIndex._minOID + i;

                        // Thử giả lập lỗi ngẫu nhiên (minh họa)
                        if (new Random().Next(0, 10) < 2) throw new Exception("Fake processing error");

                        var record = new DataRecord
                        {
                            OID = currentOID,
                            Value = $"Record-{currentOID}" // thay bằng dữ liệu thực tế
                        };

                        // Giả lập xử lý
                        await Task.Delay(5);

                        // Ghi vào DB ngay khi xử lý xong
                        var cmd = conn.CreateCommand();
                        cmd.CommandText = "INSERT OR REPLACE INTO Records (JobID, OID, Value, ProcessedAt) VALUES ($jobId, $oid, $val, $time);";
                        cmd.Parameters.AddWithValue("$jobId", _jobId);
                        cmd.Parameters.AddWithValue("$oid", record.OID);
                        cmd.Parameters.AddWithValue("$val", record.Value);
                        cmd.Parameters.AddWithValue("$time", DateTime.UtcNow.ToString("o"));
                        cmd.ExecuteNonQuery();

                        // Đánh dấu record đã xử lý
                        SetBit(ref bitArray, i, true);
                        status.LastOffset = i;
                        status.LastIndexed = DateTime.UtcNow;

                        // Checkpoint sau mỗi batch
                        if ((i + 1) % 50 == 0)
                            SaveCheckpoint(fullIndex, status, bitArray);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Lỗi khi xử lý OID={fullIndex._minOID + i}: {ex.Message}");
                    Console.WriteLine("Service sẽ tiếp tục xử lý bản ghi tiếp theo...");
                    continue; // tiếp tục bản ghi tiếp theo
                }
            }

            // Hoàn tất Job-level
            status.FullIndexingCompleted = true;
            SaveCheckpoint(fullIndex, status, bitArray);
        }

        private void SaveCheckpoint(FullIndex fullIndex, IndexingStatus status, byte[] bitArray)
        {
            if (File.Exists(_statusFile))
                File.Copy(_statusFile, _backupStatusFile, true);

            fullIndex._bitArray = Convert.ToBase64String(bitArray);
            fullIndex._arrayLength = bitArray.Length;

            File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(fullIndex, new JsonSerializerOptions { WriteIndented = true }));
            File.WriteAllText(_statusFile, JsonSerializer.Serialize(status, new JsonSerializerOptions { WriteIndented = true }));
        }

        #region BitArray Helpers
        public static bool IsBitSet(ref byte[] array, int index)
        {
            if (index >= array.Length * 8)
                array = ExpandBitArray(array, index + 1);

            int byteIndex = index / 8;
            int bitIndex = index % 8;
            return (array[byteIndex] & (1 << bitIndex)) != 0;
        }

        public static void SetBit(ref byte[] array, int index, bool value)
        {
            if (index >= array.Length * 8)
                array = ExpandBitArray(array, index + 1);

            int byteIndex = index / 8;
            int bitIndex = index % 8;

            if (value)
                array[byteIndex] |= (byte)(1 << bitIndex);
            else
                array[byteIndex] &= (byte)~(1 << bitIndex);
        }

        private static byte[] ExpandBitArray(byte[] oldArray, int requiredBits)
        {
            int requiredBytes = (requiredBits + 7) / 8;
            if (requiredBytes <= oldArray.Length) return oldArray;

            int growth = Math.Max(1024, oldArray.Length / 5);
            int newLength = Math.Max(requiredBytes, oldArray.Length + growth);

            byte[] newArray = new byte[newLength];
            Array.Copy(oldArray, newArray, oldArray.Length);
            Console.WriteLine($"⚙️ Mở rộng bitArray từ {oldArray.Length} → {newArray.Length} byte.");
            return newArray;
        }
        #endregion
    }
    #endregion

    #region JobIndexer
    public class JobIndexer
    {
        private readonly int _jobId;
        private readonly List<int> _oids;
        private readonly string _jobDir;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupStatusFile;

        public JobIndexer(int jobId, List<int> oids)
        {
            _jobId = jobId;
            _oids = oids;
            _jobDir = $@"D:\DM\PIDX\{jobId}";
            Directory.CreateDirectory(_jobDir);

            _fullIndexFile = Path.Combine(_jobDir, $"ExportJob{jobId}-fullindex.json");
            _statusFile = Path.Combine(_jobDir, $"ExportJob{jobId}-indexing-status.json");
            _backupStatusFile = Path.Combine(_jobDir, $"ExportJob{jobId}-indexing-status.json.bak");

            if (!File.Exists(_fullIndexFile))
            {
                int bytes = (_oids.Count + 7) / 8;
                var fullIndex = new FullIndex
                {
                    _utcCreated = DateTime.UtcNow,
                    _arrayLength = bytes,
                    _minOID = _oids[0],
                    _maxOID = _oids[_oids.Count - 1],
                    _bitArray = Convert.ToBase64String(new byte[bytes])
                };
                File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(fullIndex, new JsonSerializerOptions { WriteIndented = true }));

                var status = new IndexingStatus
                {
                    LastIndexed = DateTime.UtcNow,
                    LastOffset = 0,
                    FullIndexingCompleted = false
                };
                File.WriteAllText(_statusFile, JsonSerializer.Serialize(status, new JsonSerializerOptions { WriteIndented = true }));
                File.WriteAllText(_backupStatusFile, JsonSerializer.Serialize(status, new JsonSerializerOptions { WriteIndented = true }));
            }
        }

        public async Task RunAsync()
        {
            var fullIndex = JsonSerializer.Deserialize<FullIndex>(File.ReadAllText(_fullIndexFile))!;
            var status = JsonSerializer.Deserialize<IndexingStatus>(File.ReadAllText(_statusFile))!;
            byte[] bitArray = Convert.FromBase64String(fullIndex._bitArray);

            foreach (var oid in _oids)
            {
                int index = _oids.IndexOf(oid);
                try
                {
                    if (!RecordIndexer.IsBitSet(ref bitArray, index))
                    {
                        var recordIndexer = new RecordIndexer(_jobId, oid);
                        await recordIndexer.RunAsync();

                        // đánh dấu Job-level
                        RecordIndexer.SetBit(ref bitArray, index, true);
                        status.LastOffset = index;
                        status.LastIndexed = DateTime.UtcNow;

                        // checkpoint
                        SaveCheckpoint(fullIndex, status, bitArray);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Lỗi khi xử lý OID={oid}: {ex.Message}");
                    Console.WriteLine("Service sẽ tiếp tục OID tiếp theo...");
                    continue;
                }
            }

            status.FullIndexingCompleted = true;
            SaveCheckpoint(fullIndex, status, bitArray);

            // Xuất CSV từ DB cho Job hiện tại
            string dbDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DB");
            string dbFile = Path.Combine(dbDir, "ThirdSight.db");
            string csvFile = Path.Combine(_jobDir, $"Job{_jobId}-data.csv");

            using var conn = new SQLiteConnection($"Data Source={dbFile}");
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT OID, Value, ProcessedAt FROM Records WHERE JobID=$jobId ORDER BY OID;";
            cmd.Parameters.AddWithValue("$jobId", _jobId);

            using var reader = cmd.ExecuteReader();
            using var writer = new StreamWriter(csvFile, false, Encoding.UTF8);
            writer.WriteLine("OID,Value,ProcessedAt");
            while (reader.Read())
            {
                writer.WriteLine($"{reader.GetInt32(0)},{reader.GetString(1)},{reader.GetString(2)}");
            }

            Console.WriteLine($"✅ Job {_jobId} hoàn tất, CSV: {csvFile}");
        }

        private void SaveCheckpoint(FullIndex fullIndex, IndexingStatus status, byte[] bitArray)
        {
            if (File.Exists(_statusFile))
                File.Copy(_statusFile, _backupStatusFile, true);

            fullIndex._bitArray = Convert.ToBase64String(bitArray);
            fullIndex._arrayLength = bitArray.Length;

            File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(fullIndex, new JsonSerializerOptions { WriteIndented = true }));
            File.WriteAllText(_statusFile, JsonSerializer.Serialize(status, new JsonSerializerOptions { WriteIndented = true }));
        }
    }
    #endregion

    #region Program
    class Program
    {
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        static async Task Main(string[] args)
        {
            // Bắt lỗi toàn cục cho thread bình thường
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            // Bắt lỗi async Task chưa được xử lý
            TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;

            try
            {
                Console.InputEncoding = Encoding.UTF8;
                Console.OutputEncoding = Encoding.UTF8;
                log4net.Config.XmlConfigurator.Configure();

                if (args.Length < 2)
                {
                    Console.WriteLine("Cú pháp: Program.exe <JobID> <OID1,OID2,...>");
                    return;
                }

                if (!int.TryParse(args[0], out int jobId))
                {
                    Console.WriteLine("JobID không hợp lệ!");
                    return;
                }

                var oidsInput = args[1].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                List<int> oids = new();
                foreach (var s in oidsInput)
                    if (int.TryParse(s, out int oid)) oids.Add(oid);

                if (oids.Count == 0)
                {
                    Console.WriteLine("Không có OID hợp lệ!");
                    return;
                }

                var jobIndexer = new JobIndexer(jobId, oids);
                await jobIndexer.RunAsync();

                Console.WriteLine("Tất cả OID đã xử lý xong!");
                
            }
            catch (Exception ex)
            {
                log.Error(ex.Message, ex);
                // Bắt các lỗi còn sót trong Main
                Console.WriteLine("\n❌ Lỗi không mong muốn: " + ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Exception ex = (Exception)e.ExceptionObject;
            log.Error(ex.Message, ex);
            Console.WriteLine("\n🔴 UNHANDLED EXCEPTION:");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);

            if (ex.InnerException != null)
            {
                Console.WriteLine("\nInner Exception:");
                Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine(ex.InnerException.StackTrace);
            }

            Console.WriteLine("\nỨng dụng sẽ kết thúc an toàn.");
            Environment.Exit(1);
        }

        private static void TaskScheduler_UnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            Console.WriteLine("\n🔴 UNOBSERVED TASK EXCEPTION:");
            Console.WriteLine(e.Exception.Message);
            Console.WriteLine(e.Exception.StackTrace);
            e.SetObserved(); // tránh crash ứng dụng
        }
    }
    #endregion
}
