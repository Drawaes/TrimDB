using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DatabaseDeleteFacts : DatabaseTestBase
    {
        // #41
        [Fact]
        [Trait("Category", "Regression")]
        public async Task DatabaseDeleteThenGetReturnsDefault()
        {
            var key = Encoding.UTF8.GetBytes("del_key");
            var value = Encoding.UTF8.GetBytes("del_value");

            await _db.PutAsync(key, value);
            await _db.DeleteAsync(key);

            var result = await _db.GetAsync(key);
            Assert.Equal(0, result.Length);
        }

        // #42
        [Fact]
        [Trait("Category", "Regression")]
        public async Task DatabasePutDeletePutReturnsNewValue()
        {
            var key = Encoding.UTF8.GetBytes("cycle_key");
            var valueA = Encoding.UTF8.GetBytes("alpha");
            var valueB = Encoding.UTF8.GetBytes("beta");

            await _db.PutAsync(key, valueA);
            await _db.DeleteAsync(key);
            await _db.PutAsync(key, valueB);

            var result = await _db.GetAsync(key);
            Assert.Equal(valueB, result.ToArray());
        }

        // #43
        [Fact]
        [Trait("Category", "Regression")]
        public async Task DatabaseDeleteAllKeysThenGet()
        {
            var count = 50;
            for (var i = 0; i < count; i++)
            {
                var key = Encoding.UTF8.GetBytes($"bulk_key_{i:D4}");
                var value = Encoding.UTF8.GetBytes($"bulk_val_{i:D4}");
                await _db.PutAsync(key, value);
            }

            for (var i = 0; i < count; i++)
            {
                var key = Encoding.UTF8.GetBytes($"bulk_key_{i:D4}");
                await _db.DeleteAsync(key);
            }

            for (var i = 0; i < count; i++)
            {
                var key = Encoding.UTF8.GetBytes($"bulk_key_{i:D4}");
                var result = await _db.GetAsync(key);
                Assert.Equal(0, result.Length);
            }
        }

        // #44
        [Fact]
        [Trait("Category", "Regression")]
        public async Task DatabaseInterleavedPutsAndDeletes()
        {
            var keyA = Encoding.UTF8.GetBytes("key_A");
            var keyB = Encoding.UTF8.GetBytes("key_B");
            var keyC = Encoding.UTF8.GetBytes("key_C");

            await _db.PutAsync(keyA, Encoding.UTF8.GetBytes("A_original"));
            await _db.PutAsync(keyB, Encoding.UTF8.GetBytes("B_original"));
            await _db.DeleteAsync(keyA);
            await _db.PutAsync(keyC, Encoding.UTF8.GetBytes("C_value"));
            await _db.DeleteAsync(keyB);
            await _db.PutAsync(keyA, Encoding.UTF8.GetBytes("A_new"));

            var resultA = await _db.GetAsync(keyA);
            Assert.Equal(Encoding.UTF8.GetBytes("A_new"), resultA.ToArray());

            var resultB = await _db.GetAsync(keyB);
            Assert.Equal(0, resultB.Length);

            var resultC = await _db.GetAsync(keyC);
            Assert.Equal(Encoding.UTF8.GetBytes("C_value"), resultC.ToArray());
        }

        // #45 - BUG-E: DeleteAsync ignores keys already on disk
        [Fact]
        [Trait("Category", "Bug")]
        public async Task DeleteKeyThatExistsOnlyInSSTable()
        {
            // We need the key to be flushed to disk, then deleted via the API.
            // Use a small allocator to force overflow and flush.
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 100, 25))
            };

            var db = new TrimDatabase(options);
            await db.LoadAsync();
            try
            {
                // Write a known key
                var targetKey = Encoding.UTF8.GetBytes("target_key");
                var targetValue = Encoding.UTF8.GetBytes("target_value");
                await db.PutAsync(targetKey, targetValue);

                // Fill the memtable to trigger overflow and flush
                var words = CommonData.Words;
                foreach (var word in words)
                {
                    var key = Encoding.UTF8.GetBytes(word);
                    var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                    await db.PutAsync(key, value);
                }

                // Give flush time to complete
                await Task.Delay(500);

                // The target key should now be on disk. Delete it.
                // BUG-E: DeleteAsync only searches current memtable, returns false, writes no tombstone.
                await db.DeleteAsync(targetKey);

                var result = await db.GetAsync(targetKey);
                Assert.Equal(0, result.Length);
            }
            finally
            {
                try { await db.DisposeAsync(); } catch { }
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #46 - BUG-D: tombstone encoding in BlockWriter
        [Fact]
        [Trait("Category", "Bug")]
        public async Task DeleteKeyThenFlushThenGet()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 100, 25))
            };

            var db = new TrimDatabase(options);
            await db.LoadAsync();
            try
            {
                // Put a key, then delete it (tombstone in memtable)
                var key = Encoding.UTF8.GetBytes("tombstone_key");
                var value = Encoding.UTF8.GetBytes("tombstone_value");
                await db.PutAsync(key, value);
                await db.DeleteAsync(key);

                // Fill to trigger flush
                var words = CommonData.Words;
                foreach (var word in words)
                {
                    var k = Encoding.UTF8.GetBytes(word);
                    var v = Encoding.UTF8.GetBytes($"VALUE={word}");
                    await db.PutAsync(k, v);
                }

                await Task.Delay(500);

                // After flush, tombstone should persist. BUG-D: BlockWriter writes 0, not -1.
                var result = await db.GetAsync(key);
                Assert.Equal(0, result.Length);
            }
            finally
            {
                try { await db.DisposeAsync(); } catch { }
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #47
        [Fact]
        [Trait("Category", "Specification")]
        public async Task TombstoneInNewerSSTableShadowsOlderSSTable()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 100, 25))
            };

            var db = new TrimDatabase(options);
            await db.LoadAsync();
            try
            {
                // Write target key with value, flush to SSTable #1
                var targetKey = Encoding.UTF8.GetBytes("shadow_key");
                var targetValue = Encoding.UTF8.GetBytes("shadow_value");
                await db.PutAsync(targetKey, targetValue);

                // Fill to trigger flush
                var words = CommonData.Words;
                foreach (var word in words)
                {
                    await db.PutAsync(Encoding.UTF8.GetBytes(word), Encoding.UTF8.GetBytes($"V={word}"));
                }
                await Task.Delay(500);

                // Now delete the target key (creates tombstone in memtable)
                await db.DeleteAsync(targetKey);

                // Fill again to trigger second flush -- tombstone goes to SSTable #2
                foreach (var word in words)
                {
                    await db.PutAsync(
                        Encoding.UTF8.GetBytes("2_" + word),
                        Encoding.UTF8.GetBytes($"V2={word}"));
                }
                await Task.Delay(500);

                // Newer SSTable tombstone should shadow older SSTable value
                var result = await db.GetAsync(targetKey);
                Assert.Equal(0, result.Length);
            }
            finally
            {
                try { await db.DisposeAsync(); } catch { }
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }
    }
}
