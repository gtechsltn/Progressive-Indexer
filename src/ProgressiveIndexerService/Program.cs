using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SQLite;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
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
        public long _minOID { get; set; }
        public long _maxOID { get; set; }
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
        public long OID { get; set; }
        public string Value { get; set; }
    }
    #endregion

    #region RecordIndexer
    public class RecordIndexer
    {
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly int _jobId;
        private readonly long _oid;
        private readonly string _recordDir;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupStatusFile;
        private readonly string _dbFile;

        public RecordIndexer(int jobId, long oid)
        {
            _jobId = jobId;
            _oid = oid;

            _recordDir = $@"D:\DM\PIDX\{jobId}\.{oid}";
            Directory.CreateDirectory(_recordDir);

            _fullIndexFile = Path.Combine(_recordDir, $"{oid}-fullindex.json");
            _statusFile = Path.Combine(_recordDir, $"{oid}-indexing-status.json");
            _backupStatusFile = Path.Combine(_recordDir, $"{oid}-indexing-status.json.bak");

            string dbDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DB");
            Directory.CreateDirectory(dbDir);
            _dbFile = Path.Combine(dbDir, "ThirdSight.db");

            InitializeFiles();
        }

        private void InitializeFiles()
        {
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

            if (!File.Exists(_dbFile))
            {
                using var conn = new SQLiteConnection($"Data Source={_dbFile}");
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = @"
                    CREATE TABLE IF NOT EXISTS Records (
                        JobID INTEGER,
                        OID BIGINT,
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

            using var conn = new SQLiteConnection($"Data Source={_dbFile}");
            conn.Open();

            int index = 0; // Chỉ số bitArray cho record
            try
            {
                if (!IsBitSet(ref bitArray, index))
                {
                    log.Info($"Chỉ số bitArray cho record = {index}, record id = {_oid}.");


                    // ===== Giả lập lỗi critical nếu bật AppSettings =====
                    if (ConfigHelper.GetBoolean("ErrorSimulationFlag") && (_oid == ConfigHelper.GetLong("ErrorSimulationRecordId")))
                        throw new Exception($"Critical error khi xử lý OID {_oid}");

                    var record = new DataRecord
                    {
                        OID = _oid,
                        Value = $"Record-{_oid}"
                    };

                    await Task.Delay(5); // Giả lập xử lý

                    var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        INSERT OR REPLACE INTO Records (JobID, OID, Value, ProcessedAt)
                        VALUES ($jobId, $oid, $val, $time);";
                    cmd.Parameters.AddWithValue("$jobId", _jobId);
                    cmd.Parameters.AddWithValue("$oid", record.OID);
                    cmd.Parameters.AddWithValue("$val", record.Value);
                    cmd.Parameters.AddWithValue("$time", DateTime.UtcNow.ToString("o"));
                    cmd.ExecuteNonQuery();

                    SetBit(ref bitArray, index, true);
                    status.LastOffset = index;
                    status.LastIndexed = DateTime.UtcNow;
                    SaveCheckpoint(fullIndex, status, bitArray);

                    Console.WriteLine($"✅ Hoàn tất OID={_oid}");
                }
            }
            catch (Exception ex)
            {
                log.Error(ex.Message, ex);
                SaveCheckpoint(fullIndex, status, bitArray); // lưu trạng thái trước khi ném lỗi
                throw; // ném lỗi critical ra ngoài
            }
            finally
            {
                status.FullIndexingCompleted = true;
                SaveCheckpoint(fullIndex, status, bitArray);
            }
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

    #region Configuration Helpers

    public class ConfigHelper
    {
        public static bool GetBoolean(string configKey, bool defaultValue = false)
        {
            var cfgKey = ConfigurationManager.AppSettings[configKey];
            var success = bool.TryParse(cfgKey, out bool retValue);
            return success ? retValue : defaultValue;
        }

        public static int GetInt(string configKey, int defaultValue = 0)
        {
            var cfgKey = ConfigurationManager.AppSettings[configKey];
            var success = int.TryParse(cfgKey, out int retValue);
            return success ? retValue : defaultValue;
        }

        public static long GetLong(string configKey, long defaultValue = 0)
        {
            var cfgKey = ConfigurationManager.AppSettings[configKey];
            var success = long.TryParse(cfgKey, out long retValue);
            return success ? retValue : defaultValue;
        }
    }

    #endregion Configuration Helpers

    #region JobIndexer
    public class JobIndexer
    {
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly int _jobId;
        private readonly List<long> _oids;
        private readonly string _jobDir;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupStatusFile;
        public List<long> FailedOids { get; private set; } = new();

        public JobIndexer(int jobId, List<long> oids)
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
                log.Info($"Bắt đầu xử lý record id = {oid}.");
                int index = _oids.IndexOf(oid);
                try
                {
                    if (!RecordIndexer.IsBitSet(ref bitArray, index))
                    {
                        var recordIndexer = new RecordIndexer(_jobId, oid);
                        await recordIndexer.RunAsync(); // nếu lỗi critical, ném ra -> dừng

                        RecordIndexer.SetBit(ref bitArray, index, true);
                        status.LastOffset = index;
                        status.LastIndexed = DateTime.UtcNow;
                        SaveCheckpoint(fullIndex, status, bitArray);
                    }
                }
                catch (Exception ex)
                {
                    log.Error(ex.Message, ex);
                    FailedOids.Add(oid);
                    Console.WriteLine($"❌ Critical error OID={oid}: {ex.Message}");
                    SaveCheckpoint(fullIndex, status, bitArray);
                    ExportCsvFromDb(); // xuất CSV trước khi dừng
                    Environment.Exit(1); // dừng toàn bộ chương trình
                }
            }

            status.FullIndexingCompleted = true;
            SaveCheckpoint(fullIndex, status, bitArray);

            ExportCsvFromDb();
            Console.WriteLine("✅ Job hoàn tất!");
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

        private void ExportCsvFromDb()
        {
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
                writer.WriteLine($"{reader.GetInt64(0)},{reader.GetString(1)},{reader.GetString(2)}");
            }

            Console.WriteLine($"✅ CSV xuất ra: {csvFile}");
        }
    }
    #endregion

    #region Program
    class Program
    {
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        static async Task Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;

            try
            {
                Console.InputEncoding = Encoding.UTF8;
                Console.OutputEncoding = Encoding.UTF8;
                log4net.Config.XmlConfigurator.Configure();

                if (args.Length < 2)
                {
                    Console.WriteLine("Cú pháp: ProgressiveIndexerService <JobID> <OID1,OID2,...>");
                    return;
                }

                if (!int.TryParse(args[0], out int jobId))
                {
                    Console.WriteLine("JobID không hợp lệ!");
                    return;
                }

                var oidsInput = args[1].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                List<long> oids = new();
                foreach (var s in oidsInput)
                    if (long.TryParse(s, out long oid)) oids.Add(oid);

                if (oids.Count == 0)
                {
                    Console.WriteLine("Không có OID hợp lệ!");
                    return;
                }

                var jobIndexer = new JobIndexer(jobId, oids);
                await jobIndexer.RunAsync();
            }
            catch (Exception ex)
            {
                log.Error(ex.Message, ex);
                Console.WriteLine("\n❌ Lỗi không mong muốn: " + ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Exception ex = (Exception)e.ExceptionObject;
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
            e.SetObserved();
        }
    }
    #endregion
}
