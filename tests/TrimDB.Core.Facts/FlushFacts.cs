using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class FlushFacts : IAsyncLifetime
    {
        private readonly string _folder;
        private TrimDatabase _db;

        public FlushFacts()
        {
            _folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
        }

        public async Task InitializeAsync()
        {
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = _folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                DisableManifest = true,
                // Small allocator to trigger overflow quickly
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 100, 25))
            };
            _db = new TrimDatabase(options);
            await _db.LoadAsync();
        }

        public async Task DisposeAsync()
        {
            if (_db != null)
            {
                try { await _db.DisposeAsync(); } catch { }
            }
            try
            {
                if (Directory.Exists(_folder))
                    Directory.Delete(_folder, true);
            }
            catch { }
        }

        // #18
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MemtableFlushPreservesAllKeys()
        {
            var words = CommonData.Words;

            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await _db.PutAsync(key, value);
            }

            // Wait for flush to complete
            await Task.Delay(1000);

            // Every key must be findable, either in old memtable or on disk
            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var result = await _db.GetAsync(key);
                Assert.True(result.Length > 0, $"Key '{word}' not found after flush");
            }
        }

        // #19
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MemtableFlushPreservesAllValues()
        {
            var words = CommonData.Words;

            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await _db.PutAsync(key, value);
            }

            await Task.Delay(1000);

            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var expected = Encoding.UTF8.GetBytes($"VALUE={word}");
                var result = await _db.GetAsync(key);
                Assert.Equal(expected, result.ToArray());
            }
        }

        // #20 - BUG-D: BlockWriter writes value.Length (0), not -1 for tombstones
        [Fact]
        [Trait("Category", "Bug")]
        public async Task FlushDeletedKeysWriteTombstones()
        {
            // Put a key, delete it (tombstone in memtable), then flush
            var targetKey = Encoding.UTF8.GetBytes("tombstone_target");
            var targetValue = Encoding.UTF8.GetBytes("tombstone_value");

            await _db.PutAsync(targetKey, targetValue);
            await _db.DeleteAsync(targetKey);

            // Fill to trigger flush
            var words = CommonData.Words;
            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await _db.PutAsync(key, value);
            }

            await Task.Delay(1000);

            // After flush, the tombstone must survive. Key should still be "deleted"
            // (return default), not appear as an empty-value live key.
            var result = await _db.GetAsync(targetKey);
            Assert.Equal(0, result.Length);

            // Also verify the key is not returned as "found" from the SSTable
            // by putting a different key and reading back to confirm SSTable is working
            var probeKey = Encoding.UTF8.GetBytes(words[0]);
            var probeResult = await _db.GetAsync(probeKey);
            Assert.True(probeResult.Length > 0, "SSTable read is not working at all");
        }

        // #21
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MultipleMemtableFlushes()
        {
            var words = CommonData.Words;
            var batchSize = words.Length;

            // Write 3 batches, each should trigger at least one flush
            for (var batch = 0; batch < 3; batch++)
            {
                foreach (var word in words)
                {
                    var key = Encoding.UTF8.GetBytes($"b{batch}_{word}");
                    var value = Encoding.UTF8.GetBytes($"VALUE_b{batch}_{word}");
                    await _db.PutAsync(key, value);
                }
            }

            await Task.Delay(2000);

            // All data from all batches must be readable
            for (var batch = 0; batch < 3; batch++)
            {
                foreach (var word in words)
                {
                    var key = Encoding.UTF8.GetBytes($"b{batch}_{word}");
                    var expected = Encoding.UTF8.GetBytes($"VALUE_b{batch}_{word}");
                    var result = await _db.GetAsync(key);
                    Assert.Equal(expected, result.ToArray());
                }
            }
        }

        // #22
        [Fact]
        [Trait("Category", "Specification")]
        public async Task FlushDoesNotLoseInFlightWrites()
        {
            var totalKeys = 2000;
            var keys = new byte[totalKeys][];
            var values = new byte[totalKeys][];

            // Write keys in a tight loop to trigger flush mid-write
            for (var i = 0; i < totalKeys; i++)
            {
                keys[i] = Encoding.UTF8.GetBytes($"inflight_{i:D6}");
                values[i] = Encoding.UTF8.GetBytes($"val_{i:D6}");
                await _db.PutAsync(keys[i], values[i]);
            }

            await Task.Delay(1000);

            // All keys must exist
            for (var i = 0; i < totalKeys; i++)
            {
                var result = await _db.GetAsync(keys[i]);
                Assert.Equal(values[i], result.ToArray());
            }
        }
    }
}
