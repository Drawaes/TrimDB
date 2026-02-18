using System;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using TrimDB.Core;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.CachePrototype;

namespace TrimDB.Benchmarks
{
    [Config(typeof(TrimBenchConfig))]
    public class DatabaseReadBench
    {
        private TrimDatabase _db = null!;
        private string _tempFolder = null!;
        private byte[] _hitKey = null!;
        private byte[] _missKey = null!;
        private const int KeyCount = 10_000;

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _tempFolder = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "trimdb_read_bench_" + Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(_tempFolder);

            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = _tempFolder,
                DisableWAL = true,
                DisableMerging = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4 * 1024 * 1024, 25)),
                BlockCache = () => new ProtoSharded(200),
            };

            _db = new TrimDatabase(options);
            await _db.LoadAsync();

            // Populate keys
            for (int i = 0; i < KeyCount; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key_{i:D8}");
                var value = Encoding.UTF8.GetBytes($"value_{i:D8}");
                await _db.PutAsync(key, value);
            }

            // Flush to SSTable
            await _db.FlushAsync();
            await Task.Delay(200);

            _hitKey = Encoding.UTF8.GetBytes($"key_{KeyCount / 2:D8}");
            _missKey = Encoding.UTF8.GetBytes("key_zzzzzzzz_not_exist");
        }

        [GlobalCleanup]
        public async Task GlobalCleanup()
        {
            await _db.DisposeAsync();
            try { System.IO.Directory.Delete(_tempFolder, true); } catch { }
        }

        [Benchmark(Baseline = true)]
        public async Task<ReadOnlyMemory<byte>> GetAsyncFound()
        {
            return await _db.GetAsync(_hitKey);
        }

        [Benchmark]
        public async Task<ReadOnlyMemory<byte>> GetAsyncMiss()
        {
            return await _db.GetAsync(_missKey);
        }

        [Benchmark]
        public async Task GetWithLeaseAsync()
        {
            var lease = await _db.GetWithLeaseAsync(_hitKey);
            lease.Dispose();
        }

        [Benchmark]
        public async Task<int> ScanAll()
        {
            int count = 0;
            await foreach (var entry in _db.ScanAsync())
            {
                count++;
            }
            return count;
        }
    }
}
