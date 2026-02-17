using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class MultiLevelReadFacts : IAsyncLifetime
    {
        private readonly string _folder;
        private TrimDatabase _db;

        public MultiLevelReadFacts()
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
                // Small allocator to force flushes
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

        /// <summary>
        /// Fills the memtable with words to trigger a flush, pushing existing data to L1.
        /// </summary>
        private async Task TriggerFlush()
        {
            var words = CommonData.Words;
            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"filler_{word}");
                await _db.PutAsync(key, value);
            }
            // Give the background flush time to complete
            await Task.Delay(1000);
        }

        // #31
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ReadFallsThroughFromMemtableToL1()
        {
            // Write a key, then flush it to L1 by filling the memtable
            var targetKey = Encoding.UTF8.GetBytes("l1_only_key");
            var targetValue = Encoding.UTF8.GetBytes("l1_only_value");
            await _db.PutAsync(targetKey, targetValue);

            await TriggerFlush();

            // The target key should now be in L1, not in the current memtable.
            // GetAsync must search through to the storage layer.
            var result = await _db.GetAsync(targetKey);
            Assert.Equal(targetValue, result.ToArray());
        }

        // #32
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MemtableValueShadowsSSTableValue()
        {
            // Write key with old value, flush to L1
            var key = Encoding.UTF8.GetBytes("shadow_key");
            var oldValue = Encoding.UTF8.GetBytes("old_value");
            var newValue = Encoding.UTF8.GetBytes("new_value");

            await _db.PutAsync(key, oldValue);
            await TriggerFlush();

            // Now write newer value to current memtable
            await _db.PutAsync(key, newValue);

            // GetAsync should return the memtable version (newer), not the SSTable version
            var result = await _db.GetAsync(key);
            Assert.Equal(newValue, result.ToArray());
        }

        // #33
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MemtableTombstoneShadowsSSTableValue()
        {
            // Write key, flush to L1
            var key = Encoding.UTF8.GetBytes("tombstone_shadow_key");
            var value = Encoding.UTF8.GetBytes("will_be_deleted");

            await _db.PutAsync(key, value);
            await TriggerFlush();

            // Delete in current memtable (creates tombstone)
            // BUG-E: DeleteAsync only checks current memtable. If key is not there, no tombstone written.
            // Even if we put-then-delete in memtable, the tombstone should shadow the SSTable value.
            await _db.PutAsync(key, Encoding.UTF8.GetBytes("temporary"));
            await _db.DeleteAsync(key);

            var result = await _db.GetAsync(key);
            Assert.Equal(0, result.Length);
        }

        // #34
        [Fact]
        [Trait("Category", "Specification")]
        public async Task OldMemtableSearchedBeforeSSTable()
        {
            // Write a key to the memtable
            var key = Encoding.UTF8.GetBytes("pending_flush_key");
            var value = Encoding.UTF8.GetBytes("pending_flush_value");
            await _db.PutAsync(key, value);

            // Fill the memtable to trigger overflow. The key should move to _oldInMemoryTables
            // (pending flush) and be searchable there.
            var words = CommonData.Words;
            foreach (var word in words)
            {
                await _db.PutAsync(
                    Encoding.UTF8.GetBytes(word),
                    Encoding.UTF8.GetBytes($"V={word}"));
            }

            // Don't wait for flush to complete. The key should be in _oldInMemoryTables.
            var result = await _db.GetAsync(key);
            Assert.Equal(value, result.ToArray());
        }
    }
}
