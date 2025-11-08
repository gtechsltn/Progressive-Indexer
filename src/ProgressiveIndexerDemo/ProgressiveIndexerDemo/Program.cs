using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using log4net;


namespace ProgressiveIndexerJobAndRecord
{
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

    // ----------------- Record-level indexer -----------------
    public class RecordIndexer
    {
        private readonly string _recordDir;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupStatusFile;

        private const int BatchSize = 50;
        private readonly int _oid;

        public RecordIndexer(int jobId, int oid)
        {
            _oid = oid;
            _recordDir = $@"D:\DM\PIDX\{jobId}\.{oid}";
            Directory.CreateDirectory(_recordDir);

            _fullIndexFile = Path.Combine(_recordDir, $"{oid}-fullindex.json");
            _statusFile = Path.Combine(_recordDir, $"{oid}-indexing-status.json");
            _backupStatusFile = Path.Combine(_recordDir, $"{oid}-indexing-status.json.bak");

            InitializeFiles();
        }

        private void InitializeFiles()
        {
            if (!File.Exists(_fullIndexFile))
            {
                var init = new FullIndex
                {
                    _utcCreated = DateTime.UtcNow,
                    _arrayLength = 1,
                    _minOID = _oid,
                    _maxOID = _oid,
                    _bitArray = Convert.ToBase64String(new byte[] { 0 })
                };
                File.WriteAllText(_fullIndexFile, JsonSerializer.Serialize(init, new JsonSerializerOptions { WriteIndented = true }));

                var initStatus = new IndexingStatus
                {
                    LastIndexed = DateTime.UtcNow,
                    LastOffset = 0,
                    FullIndexingCompleted = false
                };
                var json = JsonSerializer.Serialize(initStatus, new JsonSerializerOptions { WriteIndented = true });
                File.WriteAllText(_statusFile, json);
                File.WriteAllText(_backupStatusFile, json);
            }
        }

        public async Task<List<DataRecord>> RunAsync()
        {
            var fullIndex = LoadFullIndex();
            var status = LoadStatus();
            byte[] bitArray = Convert.FromBase64String(fullIndex._bitArray);

            List<DataRecord> records = new();
            int totalBits = fullIndex._arrayLength * 8;

            for (int i = status.LastOffset; i < totalBits; i++)
            {
                if (!IsBitSet(ref bitArray, i))
                {
                    int currentOID = fullIndex._minOID + i;
                    var record = new DataRecord
                    {
                        OID = currentOID,
                        Value = $"Record-{currentOID}"
                    };

                    await Task.Delay(5); // mô phỏng xử lý
                    records.Add(record);

                    SetBit(ref bitArray, i, true);
                    status.LastOffset = i;
                    status.LastIndexed = DateTime.UtcNow;

                    if ((i + 1) % BatchSize == 0)
                        SaveCheckpoint(fullIndex, status, bitArray);
                }
            }

            status.FullIndexingCompleted = true;
            SaveCheckpoint(fullIndex, status, bitArray);

            return records;
        }

        private FullIndex LoadFullIndex() => JsonSerializer.Deserialize<FullIndex>(File.ReadAllText(_fullIndexFile))!;
        private IndexingStatus LoadStatus()
        {
            try { return JsonSerializer.Deserialize<IndexingStatus>(File.ReadAllText(_statusFile))!; }
            catch { return JsonSerializer.Deserialize<IndexingStatus>(File.ReadAllText(_backupStatusFile))!; }
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

        public static bool IsBitSet(ref byte[] array, int index)
        {
            if (index >= array.Length * 8)
                array = ExpandBitArray(array, index + 1);
            int b = index / 8, bit = index % 8;
            return (array[b] & (1 << bit)) != 0;
        }

        public static void SetBit(ref byte[] array, int index, bool value)
        {
            if (index >= array.Length * 8)
                array = ExpandBitArray(array, index + 1);
            int b = index / 8, bit = index % 8;
            if (value) array[b] |= (byte)(1 << bit);
            else array[b] &= (byte)~(1 << bit);
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
    }

    // ----------------- Job-level indexer -----------------
    public class JobIndexer
    {
        private readonly string _jobDir;
        private readonly string _fullIndexFile;
        private readonly string _statusFile;
        private readonly string _backupStatusFile;
        private readonly List<int> _oids;
        private const int BatchSize = 50;

        public JobIndexer(int jobId, List<int> oids)
        {
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
                    _maxOID = _oids[_oids.Count - 1], // => _maxOID = _oids[^1],
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

        public async Task RunAsync(int jobId)
        {
            var fullIndex = JsonSerializer.Deserialize<FullIndex>(File.ReadAllText(_fullIndexFile))!;
            var status = JsonSerializer.Deserialize<IndexingStatus>(File.ReadAllText(_statusFile))!;
            byte[] bitArray = Convert.FromBase64String(fullIndex._bitArray);

            List<DataRecord> allRecords = new();

            for (int i = status.LastOffset; i < _oids.Count; i++)
            {
                if (!RecordIndexer.IsBitSet(ref bitArray, i))
                {
                    int oid = _oids[i];
                    var recordIndexer = new RecordIndexer(jobId, oid);
                    var records = await recordIndexer.RunAsync();
                    allRecords.AddRange(records);

                    // cập nhật bitArray của Job-level
                    RecordIndexer.SetBit(ref bitArray, i, true);
                    status.LastOffset = i;
                    status.LastIndexed = DateTime.UtcNow;

                    if ((i + 1) % BatchSize == 0)
                        SaveCheckpoint(fullIndex, status, bitArray);
                }
            }

            status.FullIndexingCompleted = true;
            SaveCheckpoint(fullIndex, status, bitArray);

            // Xuất CSV tổng hợp Job
            string csvFile = Path.Combine(_jobDir, $"Job{jobId}-data.csv");
            using var writer = new StreamWriter(csvFile, false, Encoding.UTF8);
            writer.WriteLine("OID,Value");
            foreach (var r in allRecords)
                writer.WriteLine($"{r.OID},{r.Value}");

            Console.WriteLine($"✅ Job {jobId} hoàn tất. CSV: {csvFile}");
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

    // ----------------- Program -----------------
    class Program
    {
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        static async Task Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
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
                Console.WriteLine("Không có OID hợp lệ để xử lý!");
                return;
            }

            var jobIndexer = new JobIndexer(jobId, oids);
            await jobIndexer.RunAsync(jobId);

            Console.WriteLine("Tất cả OID đã xử lý xong!");
            Console.ReadKey();
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
}
