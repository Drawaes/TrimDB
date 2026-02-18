using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DatabaseLifecycleFacts
    {
        private static TrimDatabaseOptions MakeOptions(string folder)
        {
            return new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 1000, 25))
            };
        }

        private static async Task<TrimDatabase> CreateAndLoad(string folder)
        {
            var db = new TrimDatabase(MakeOptions(folder));
            await db.LoadAsync();
            return db;
        }

        // #11 - BUG-H: DisposeAsync schedules save of null memtable
        [Fact]
        [Trait("Category", "Bug")]
        public async Task CreateEmptyDatabaseAndDispose()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                var db = await CreateAndLoad(folder);
                // No operations -- just dispose. Must not throw.
                await db.DisposeAsync();
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #12
        [Fact]
        [Trait("Category", "Specification")]
        public async Task DisposeFlushesMemtable()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                var db = await CreateAndLoad(folder);
                for (var i = 0; i < 10; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key_{i}");
                    var value = Encoding.UTF8.GetBytes($"val_{i}");
                    await db.PutAsync(key, value);
                }
                await db.DisposeAsync();

                // After dispose, there should be SSTable files on disk
                var trimFiles = Directory.GetFiles(folder, "*.trim");
                Assert.NotEmpty(trimFiles);
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #13 - BUG-J: ReadOnly mode NullReferenceException
        [Fact]
        [Trait("Category", "Bug")]
        public async Task OpenReadOnlyDoesNotCrashOnGet()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                var options = MakeOptions(folder);
                options.OpenReadOnly = true;

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                // GetAsync must not throw NullReferenceException. Should return default.
                var key = Encoding.UTF8.GetBytes("any_key");
                var result = await db.GetAsync(key);
                Assert.Equal(0, result.Length);
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #14 - BUG-I: operations after dispose throw NullRef instead of ObjectDisposedException
        [Fact]
        [Trait("Category", "Bug")]
        public async Task OperationsAfterDisposeThrow()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                var db = await CreateAndLoad(folder);
                await db.DisposeAsync();

                var key = Encoding.UTF8.GetBytes("any_key");
                var value = Encoding.UTF8.GetBytes("any_value");

                await Assert.ThrowsAsync<ObjectDisposedException>(async () => await db.PutAsync(key, value));
                await Assert.ThrowsAsync<ObjectDisposedException>(async () => await db.GetAsync(key));
                await Assert.ThrowsAsync<ObjectDisposedException>(async () => await db.DeleteAsync(key));
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #15 - BUG-H: double dispose NullReferenceException
        [Fact]
        [Trait("Category", "Bug")]
        public async Task DoubleDisposeDoesNotThrow()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                var db = await CreateAndLoad(folder);
                await db.DisposeAsync();
                // Second dispose must be a no-op, not throw NullRef
                await db.DisposeAsync();
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #16
        [Fact]
        [Trait("Category", "Regression")]
        public async Task DatabaseCreatesDirectoryIfMissing()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"), "nested", "deep");
            try
            {
                var db = await CreateAndLoad(folder);
                Assert.True(Directory.Exists(folder));
                await db.DisposeAsync();
            }
            finally
            {
                try
                {
                    // Clean up to the GUID-named root
                    var root = Path.GetDirectoryName(Path.GetDirectoryName(folder));
                    if (root != null && Directory.Exists(root))
                        Directory.Delete(root, true);
                }
                catch { }
            }
        }

        // #17
        [Fact]
        [Trait("Category", "Regression")]
        public async Task EmptyDatabaseGetReturnsDefault()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                var db = await CreateAndLoad(folder);
                var key = Encoding.UTF8.GetBytes("nothing_here");
                var result = await db.GetAsync(key);
                Assert.Equal(0, result.Length);
                await db.DisposeAsync();
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }
    }
}
