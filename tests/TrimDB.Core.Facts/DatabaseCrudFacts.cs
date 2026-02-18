using System;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DatabaseCrudFacts : DatabaseTestBase
    {
        // #1
        [Fact]
        [Trait("Category", "Specification")]
        public async Task PutAndGetSingleKey()
        {
            var key = Encoding.UTF8.GetBytes("hello");
            var value = Encoding.UTF8.GetBytes("world");

            await _db.PutAsync(key, value);
            var result = await _db.GetAsync(key);

            Assert.Equal(value, result.ToArray());
        }

        // #2
        [Fact]
        [Trait("Category", "Specification")]
        public async Task PutAndGetMultipleKeys()
        {
            for (var i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key_{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value_{i:D4}");
                await _db.PutAsync(key, value);
            }

            for (var i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key_{i:D4}");
                var expected = Encoding.UTF8.GetBytes($"value_{i:D4}");
                var result = await _db.GetAsync(key);
                Assert.Equal(expected, result.ToArray());
            }
        }

        // #3
        [Fact]
        [Trait("Category", "Regression")]
        public async Task GetNonExistentKeyReturnsDefault()
        {
            var key = Encoding.UTF8.GetBytes("does_not_exist");
            var result = await _db.GetAsync(key);
            Assert.Equal(0, result.Length);
        }

        // #4
        [Fact]
        [Trait("Category", "Regression")]
        public async Task PutOverwritesExistingKey()
        {
            var key = Encoding.UTF8.GetBytes("overwrite_me");
            var valueA = Encoding.UTF8.GetBytes("first");
            var valueB = Encoding.UTF8.GetBytes("second");

            await _db.PutAsync(key, valueA);
            await _db.PutAsync(key, valueB);

            var result = await _db.GetAsync(key);
            Assert.Equal(valueB, result.ToArray());
        }

        // #5
        [Fact]
        [Trait("Category", "Regression")]
        public async Task DeleteThenGetReturnsDefault()
        {
            var key = Encoding.UTF8.GetBytes("delete_me");
            var value = Encoding.UTF8.GetBytes("gone_soon");

            await _db.PutAsync(key, value);
            await _db.DeleteAsync(key);

            var result = await _db.GetAsync(key);
            Assert.Equal(0, result.Length);
        }

        // #6
        [Fact]
        [Trait("Category", "Regression")]
        public async Task DeleteNonExistentKeyDoesNotCrash()
        {
            var key = Encoding.UTF8.GetBytes("never_existed");
            // In an LSM-tree, Delete always writes a tombstone, so it returns true
            var deleted = await _db.DeleteAsync(key);
            Assert.True(deleted);
        }

        // #7
        [Fact]
        [Trait("Category", "Regression")]
        public async Task PutDeletePutGetReturnsNewValue()
        {
            var key = Encoding.UTF8.GetBytes("resilient_key");
            var valueA = Encoding.UTF8.GetBytes("alpha");
            var valueB = Encoding.UTF8.GetBytes("beta");

            await _db.PutAsync(key, valueA);
            await _db.DeleteAsync(key);
            await _db.PutAsync(key, valueB);

            var result = await _db.GetAsync(key);
            Assert.Equal(valueB, result.ToArray());
        }

        // #8
        [Fact]
        [Trait("Category", "Specification")]
        public async Task PutEmptyValue()
        {
            var key = Encoding.UTF8.GetBytes("empty_value_key");
            var value = Array.Empty<byte>();

            await _db.PutAsync(key, value);

            // At minimum, the put must not throw and get must not throw.
            // The API cannot distinguish "empty value" from "not found" since both
            // return zero-length ReadOnlyMemory<byte>. We verify no exception.
            var result = await _db.GetAsync(key);
            // result.Length will be 0 whether found-empty or not-found. Just don't crash.
        }

        // #9 - BUG-A: empty key collides with block sentinel
        [Fact]
        [Trait("Category", "Bug")]
        public async Task EmptyKeyCollidesWithBlockSentinel()
        {
            var key = Array.Empty<byte>();
            var value = Encoding.UTF8.GetBytes("value_for_empty_key");

            await _db.PutAsync(key, value);

            // At memtable level this should work (skip list uses length-prefixed keys).
            // After flush to SSTable, BUG-A makes the key invisible.
            var result = await _db.GetAsync(key);
            Assert.Equal(value, result.ToArray());
        }

        // #10
        [Fact]
        [Trait("Category", "Specification")]
        public async Task PutLargeValue()
        {
            // Use a larger allocator so the 1MB value fits in the memtable
            var folder = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(16 * 1024 * 1024, 25))
            };

            var db = new TrimDatabase(options);
            await db.LoadAsync();
            try
            {
                var key = Encoding.UTF8.GetBytes("large_key");
                var value = new byte[1024 * 1024]; // 1MB
                new Random(42).NextBytes(value);

                await db.PutAsync(key, value);

                var result = await db.GetAsync(key);
                Assert.Equal(value, result.ToArray());
            }
            finally
            {
                try { await db.DisposeAsync(); } catch { }
                try
                {
                    if (System.IO.Directory.Exists(folder))
                        System.IO.Directory.Delete(folder, true);
                }
                catch { }
            }
        }
    }
}
