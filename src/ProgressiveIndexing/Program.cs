using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SQLite;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using log4net;

namespace ProgressiveIndexing
{
    #region Models
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
        public long LastOffset { get; set; }
        public bool FullIndexingCompleted { get; set; }
    }
    #endregion

    #region Helpers
    public static class BitHelper
    {
        public static bool IsBitSet(ref byte[] array, long index)
        {
            if (index >= array.Length * 8)
                array = Expand(array, index + 1);
            int byteIndex = (int)(index / 8);
            int bitIndex = (int)(index % 8);
            return (array[byteIndex] & (1 << bitIndex)) != 0;
        }

        public static void SetBit(ref byte[] array, long index, bool value)
        {
            if (index >= array.Length * 8)
                array = Expand(array, index + 1);
            int byteIndex = (int)(index / 8);
            int bitIndex = (int)(index % 8);
            if (value)
                array[byteIndex] |= (byte)(1 << bitIndex);
            else
                array[byteIndex] &= (byte)~(1 << bitIndex);
        }

        private static byte[] Expand(byte[] oldArray, long requiredBits)
        {
            int requiredBytes = (int)((requiredBits + 7) / 8);
            if (requiredBytes <= oldArray.Length) return oldArray;
            byte[] newArr = new byte[requiredBytes];
            Array.Copy(oldArray, newArr, oldArray.Length);
            Console.WriteLine($"⚙️  Mở rộng bitArray từ {oldArray.Length} → {newArr.Length} byte.");
            return newArr;
        }
    }
    #endregion

    #region Folder Indexer
    public class FolderIndexer
    {
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly int _jobId;
        private readonly long _folderId;
        private readonly List<long> _recordIds;
        private readonly string _folderPath;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupFile;
        private readonly SQLiteConnection _conn;

        public FolderIndexer(int jobId, long folderId, List<long> recordIds, SQLiteConnection conn)
        {
            _jobId = jobId;
            _folderId = folderId;
            _recordIds = recordIds;
            _conn = conn;

            _folderPath = $@"D:\DM\PIDX\{jobId}\.{folderId}";
            Directory.CreateDirectory(_folderPath);
            _fullIndexFile = Path.Combine(_folderPath, $"{folderId}-fullindex.json");
            _statusFile = Path.Combine(_folderPath, $"{folderId}-indexing-status.json");
            _backupFile = Path.Combine(_folderPath, $"{folderId}-indexing-status.json.bak");

            InitFiles();
        }

        private void InitFiles()
        {
            if (!File.Exists(_fullIndexFile))
            {
                var fi = new FullIndex
                {
                    _utcCreated = DateTime.UtcNow,
                    _arrayLength = 1,
                    _minOID = _recordIds[0],
                    _maxOID = _recordIds[_recordIds.Count - 1],
                    _bitArray = Convert.ToBase64String(new byte[1])
                };
                File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(fi, new JsonSerializerOptions { WriteIndented = true }));
                var st = new IndexingStatus { LastIndexed = DateTime.UtcNow, LastOffset = 0, FullIndexingCompleted = false };
                var js = JsonSerializer.Serialize(st, new JsonSerializerOptions { WriteIndented = true });
                File.WriteAllText(_statusFile, js);
                File.WriteAllText(_backupFile, js);
            }
        }

        public async Task RunAsync(bool wantToThrowError, long failRecordId)
        {
            var fullIndex = JsonSerializer.Deserialize<FullIndex>(File.ReadAllText(_fullIndexFile))!;
            var status = JsonSerializer.Deserialize<IndexingStatus>(File.ReadAllText(_statusFile))!;
            byte[] bits = Convert.FromBase64String(fullIndex._bitArray);

            for (int i = 0; i < _recordIds.Count; i++)
            {
                long recordId = _recordIds[i];

                log.Info($"----Xử lý RecordId={recordId}...");

                if (BitHelper.IsBitSet(ref bits, i))
                {
                    log.Info($"------Đã xử lý thành công RecordId={recordId} trước đó rồi.");
                    continue;
                }

                try
                {
                    // Giả lập lỗi
                    if (wantToThrowError && recordId == failRecordId)
                        throw new Exception($"Giả lập lỗi tại Record {recordId}");

                    await Task.Delay(5); // giả lập xử lý

                    var cmd = _conn.CreateCommand();
                    cmd.CommandText = @"INSERT OR REPLACE INTO Folders(JobId, FolderId, RecordId, Status)
                                        VALUES($jid,$fid,$rid,'Done');";
                    cmd.Parameters.AddWithValue("$jid", _jobId);
                    cmd.Parameters.AddWithValue("$fid", _folderId);
                    cmd.Parameters.AddWithValue("$rid", recordId);
                    cmd.ExecuteNonQuery();

                    BitHelper.SetBit(ref bits, i, true);
                    status.LastOffset = i;
                    status.LastIndexed = DateTime.UtcNow;
                    SaveCheckpoint(fullIndex, status, bits);

                    log.Info($"------Xử lý thành công RecordId={recordId}.");
                    Console.WriteLine($"✅ Folder {_folderId} - Record {recordId} done");
                }
                catch (Exception ex)
                {
                    log.Error(ex.Message, ex);
                    Console.WriteLine($"❌ Lỗi Folder {_folderId}, Record {recordId}: {ex.Message}");
                    UpdateFolderError(recordId);
                    throw; // fail-fast
                }
            }

            status.FullIndexingCompleted = true;
            SaveCheckpoint(fullIndex, status, bits);
        }

        private void UpdateFolderError(long recordId)
        {
            var cmd = _conn.CreateCommand();
            cmd.CommandText = @"INSERT OR REPLACE INTO Folders(JobId, FolderId, RecordId, Status)
                                VALUES($jid,$fid,$rid,'Error');";
            cmd.Parameters.AddWithValue("$jid", _jobId);
            cmd.Parameters.AddWithValue("$fid", _folderId);
            cmd.Parameters.AddWithValue("$rid", recordId);
            cmd.ExecuteNonQuery();
        }

        private void SaveCheckpoint(FullIndex fi, IndexingStatus st, byte[] bits)
        {
            if (File.Exists(_statusFile))
                File.Copy(_statusFile, _backupFile, true);
            fi._bitArray = Convert.ToBase64String(bits);
            fi._arrayLength = bits.Length;
            File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(fi, new JsonSerializerOptions { WriteIndented = true }));
            File.WriteAllText(_statusFile, JsonSerializer.Serialize(st, new JsonSerializerOptions { WriteIndented = true }));
        }
    }
    #endregion

    #region JobIndexer
    public class JobIndexer
    {
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly int _jobId;
        private readonly Dictionary<long, List<long>> _folders;
        private readonly string _jobDir;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupFile;
        private readonly string _dbFile;

        public JobIndexer(int jobId, Dictionary<long, List<long>> folders)
        {
            _jobId = jobId;
            _folders = folders;

            _jobDir = $@"D:\DM\PIDX\{jobId}";
            Directory.CreateDirectory(_jobDir);

            _fullIndexFile = Path.Combine(_jobDir, $"ExportJob{jobId}-fullindex.json");
            _statusFile = Path.Combine(_jobDir, $"ExportJob{jobId}-indexing-status.json");
            _backupFile = Path.Combine(_jobDir, $"ExportJob{jobId}-indexing-status.json.bak");

            string dbDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DB");
            Directory.CreateDirectory(dbDir);
            _dbFile = Path.Combine(dbDir, "ThirdSight.db");

            InitDb();
            InitFiles();
            InitStructure();
        }

        private void InitDb()
        {
            using var conn = new SQLiteConnection($"Data Source={_dbFile}");
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS Job(JobId INTEGER PRIMARY KEY, Name TEXT, Status TEXT);
                CREATE TABLE IF NOT EXISTS Folders(JobId INTEGER, FolderId INTEGER, RecordId INTEGER, Status TEXT);
            ";
            cmd.ExecuteNonQuery();
        }

        private void InitFiles()
        {
            if (!File.Exists(_fullIndexFile))
            {
                var fi = new FullIndex
                {
                    _utcCreated = DateTime.UtcNow,
                    _arrayLength = 1,
                    _minOID = 0,
                    _maxOID = _folders.Count,
                    _bitArray = Convert.ToBase64String(new byte[1])
                };
                File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(fi, new JsonSerializerOptions { WriteIndented = true }));

                var st = new IndexingStatus { LastIndexed = DateTime.UtcNow, LastOffset = 0, FullIndexingCompleted = false };
                var js = JsonSerializer.Serialize(st, new JsonSerializerOptions { WriteIndented = true });
                File.WriteAllText(_statusFile, js);
                File.WriteAllText(_backupFile, js);
            }
        }

        private void InitStructure()
        {
            Console.WriteLine($"🗂️  Khởi tạo cấu trúc thư mục cho Job {_jobId}...");
            foreach (var folder in _folders)
            {
                string folderPath = Path.Combine(_jobDir, $".{folder.Key}");
                Directory.CreateDirectory(folderPath);

                string metaFile = Path.Combine(folderPath, "metadata.json");
                if (!File.Exists(metaFile))
                {
                    var meta = new
                    {
                        JobId = _jobId,
                        FolderId = folder.Key,
                        RecordCount = folder.Value.Count,
                        CreatedAt = DateTime.UtcNow
                    };
                    File.WriteAllText(metaFile, JsonSerializer.Serialize(meta, new JsonSerializerOptions { WriteIndented = true }));
                }
            }
            Console.WriteLine("✅ Đã khởi tạo đầy đủ cấu trúc thư mục Job.");
        }

        public async Task RunAsync(bool wantToThrowError, long failRecordId)
        {
            using var conn = new SQLiteConnection($"Data Source={_dbFile}");
            conn.Open();

            // Update Job Status
            var jobCmd = conn.CreateCommand();
            jobCmd.CommandText = "INSERT OR REPLACE INTO Job(JobId, Name, Status) VALUES($jid, $name, 'Running');";
            jobCmd.Parameters.AddWithValue("$jid", _jobId);
            jobCmd.Parameters.AddWithValue("$name", $"ExportJob{_jobId}");
            jobCmd.ExecuteNonQuery();

            try
            {
                foreach (var folder in _folders)
                {
                    log.Info($"--Xử lý JobId={_jobId}, FolderId={folder.Key}...");
                    var folderIndexer = new FolderIndexer(_jobId, folder.Key, folder.Value, conn);
                    await folderIndexer.RunAsync(wantToThrowError, failRecordId);
                }

                jobCmd.CommandText = "UPDATE Job SET Status='Completed' WHERE JobId=$jid;";
                jobCmd.ExecuteNonQuery();

                ExportCsv(conn);

                Console.WriteLine("✅ Job hoàn tất toàn bộ!");
            }
            catch (Exception ex)
            {
                log.Error(ex.Message, ex);
                Console.WriteLine($"❌ Lỗi job {_jobId}: {ex.Message}");
                var failCmd = conn.CreateCommand();
                failCmd.CommandText = "UPDATE Job SET Status='Failed' WHERE JobId=$jid;";
                failCmd.Parameters.AddWithValue("$jid", _jobId);
                failCmd.ExecuteNonQuery();
                Environment.Exit(1); // Fail-fast
            }
        }

        private void ExportCsv(SQLiteConnection conn)
        {
            string csvFile = Path.Combine(_jobDir, $"Job{_jobId}-data.csv");
            using var writer = new StreamWriter(csvFile, false, Encoding.UTF8);
            writer.WriteLine("JobId,FolderId,RecordId,Status");

            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT JobId, FolderId, RecordId, Status FROM Folders WHERE JobId=$jid ORDER BY FolderId, RecordId;";
            cmd.Parameters.AddWithValue("$jid", _jobId);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                writer.WriteLine($"{reader.GetInt32(0)},{reader.GetInt64(1)},{reader.GetInt64(2)},{reader.GetString(3)}");
            }
            Console.WriteLine($"✅ CSV xuất ra: {csvFile}");
        }
    }
    #endregion

    #region ConfigHelper
    public class ConfigHelper
    {
        public static bool GetBoolean(string key, bool defaultValue = false)
        {
            string? val = ConfigurationManager.AppSettings[key];
            if (bool.TryParse(val, out bool result)) return result;
            return defaultValue;
        }

        public static int GetInt(string key, int defaultValue = 0)
        {
            string? val = ConfigurationManager.AppSettings[key];
            if (int.TryParse(val, out int result)) return result;
            return defaultValue;
        }

        public static long GetLong(string key, long defaultValue = 0)
        {
            string? val = ConfigurationManager.AppSettings[key];
            if (long.TryParse(val, out long result)) return result;
            return defaultValue;
        }
    }
    #endregion

    #region Program
    public class Program
    {
        public static async Task Main()
        {
            Console.OutputEncoding = Encoding.UTF8;

            // Read config or defaults
            int jobId = ConfigHelper.GetInt("JobId", 2);
            bool wantError = ConfigHelper.GetBoolean("WantToThrowError", true);
            long failRecordId = ConfigHelper.GetLong("FailRecordId", 576212);

            var folders = new Dictionary<long, List<long>>
            {
                { 576210, new List<long>{ 37174 } },
                { 576302, new List<long>{ 37173, 57617, 576212 } },
                { 576303, new List<long>{ 37175 } },
            };

            var job = new JobIndexer(jobId, folders);
            await job.RunAsync(wantError, failRecordId);
        }
    }
    #endregion
}
