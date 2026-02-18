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
    public class DatabaseWriteBench
    {
        private TrimDatabase _dbNoWal = null!;
        private TrimDatabase _dbWal = null!;
        private string _tempFolderNoWal = null!;
        private string _tempFolderWal = null!;
        private byte[] _key = null!;
        private byte[] _value = null!;
        private int _counter;

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _tempFolderNoWal = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "trimdb_write_nowal_" + Guid.NewGuid().ToString("N"));
            _tempFolderWal = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "trimdb_write_wal_" + Guid.NewGuid().ToString("N"));

            _dbNoWal = new TrimDatabase(new TrimDatabaseOptions
            {
                DatabaseFolder = _tempFolderNoWal,
                DisableWAL = true,
                DisableMerging = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(64 * 1024 * 1024, 25)),
                BlockCache = () => new ProtoSharded(50),
            });
            await _dbNoWal.LoadAsync();

            _dbWal = new TrimDatabase(new TrimDatabaseOptions
            {
                DatabaseFolder = _tempFolderWal,
                DisableWAL = false,
                DisableMerging = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(64 * 1024 * 1024, 25)),
                BlockCache = () => new ProtoSharded(50),
            });
            await _dbWal.LoadAsync();

            _value = Encoding.UTF8.GetBytes("benchmark_value_data_1234567890");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _counter++;
            _key = Encoding.UTF8.GetBytes($"bench_key_{_counter:D10}");
        }

        [GlobalCleanup]
        public async Task GlobalCleanup()
        {
            await _dbNoWal.DisposeAsync();
            await _dbWal.DisposeAsync();
            try { System.IO.Directory.Delete(_tempFolderNoWal, true); } catch { }
            try { System.IO.Directory.Delete(_tempFolderWal, true); } catch { }
        }

        [Benchmark(Baseline = true)]
        public async Task PutNoWal()
        {
            await _dbNoWal.PutAsync(_key, _value);
        }

        [Benchmark]
        public async Task PutWithWal()
        {
            await _dbWal.PutAsync(_key, _value);
        }

        [Benchmark]
        public ValueTask<bool> DeleteNoWal()
        {
            return _dbNoWal.DeleteAsync(_key);
        }

        [Benchmark]
        public async Task BatchNoWal()
        {
            var batch = new WriteBatch();
            for (int i = 0; i < 10; i++)
            {
                var batchKey = Encoding.UTF8.GetBytes($"batch_{_counter}_{i:D4}");
                batch.Put(batchKey, _value);
            }
            await _dbNoWal.ApplyBatchAsync(batch);
        }

        [Benchmark]
        public async Task BatchWithWal()
        {
            var batch = new WriteBatch();
            for (int i = 0; i < 10; i++)
            {
                var batchKey = Encoding.UTF8.GetBytes($"batch_{_counter}_{i:D4}");
                batch.Put(batchKey, _value);
            }
            await _dbWal.ApplyBatchAsync(batch);
        }
    }
}
