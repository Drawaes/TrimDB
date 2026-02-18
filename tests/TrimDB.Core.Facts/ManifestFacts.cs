using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class ManifestFacts
    {
        private static string CreateTempFolder()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_Manifest_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(folder);
            return folder;
        }

        private static void CleanupFolder(string folder)
        {
            if (Directory.Exists(folder))
            {
                try { Directory.Delete(folder, true); } catch { }
            }
        }

        private static TrimDatabaseOptions MakeOptions(string folder, bool disableManifest = false, int allocatorSize = 4096 * 100)
        {
            return new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                DisableManifest = disableManifest,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(allocatorSize, 25))
            };
        }

        // --- ManifestManager unit tests ---

        [Fact]
        [Trait("Category", "Specification")]
        public async Task ManifestRoundTrip()
        {
            var folder = CreateTempFolder();
            try
            {
                var manager = new ManifestManager(folder);

                var data = new ManifestData();
                data.AddFile(1, 0);
                data.AddFile(1, 1);
                data.AddFile(2, 0);
                data.AddFile(3, 5);
                data.AddFile(3, 7);
                data.AddFile(3, 12);

                await manager.WriteAsync(data);

                Assert.True(manager.Exists);
                var readBack = manager.TryRead();
                Assert.NotNull(readBack);

                Assert.Equal(new[] { 0, 1 }, readBack!.GetFiles(1).OrderBy(x => x).ToArray());
                Assert.Equal(new[] { 0 }, readBack.GetFiles(2).ToArray());
                Assert.Equal(new[] { 5, 7, 12 }, readBack.GetFiles(3).OrderBy(x => x).ToArray());
                Assert.Empty(readBack.GetFiles(4)); // non-existent level
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Specification")]
        public async Task ManifestEmptyRoundTrip()
        {
            var folder = CreateTempFolder();
            try
            {
                var manager = new ManifestManager(folder);
                var data = new ManifestData();

                await manager.WriteAsync(data);

                var readBack = manager.TryRead();
                Assert.NotNull(readBack);
                Assert.Empty(readBack!.Levels);
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Regression")]
        public void ManifestTryReadReturnsNullWhenMissing()
        {
            var folder = CreateTempFolder();
            try
            {
                var manager = new ManifestManager(folder);
                Assert.False(manager.Exists);
                Assert.Null(manager.TryRead());
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Regression")]
        public async Task ManifestCorruptCrcThrows()
        {
            var folder = CreateTempFolder();
            try
            {
                var manager = new ManifestManager(folder);
                var data = new ManifestData();
                data.AddFile(1, 0);
                await manager.WriteAsync(data);

                // Corrupt a byte in the middle of the manifest
                var path = Path.Combine(folder, "manifest.mf");
                var bytes = File.ReadAllBytes(path);
                bytes[8] ^= 0xFF; // flip bits in the version/levelCount area
                File.WriteAllBytes(path, bytes);

                Assert.Throws<InvalidDataException>(() => manager.TryRead());
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Regression")]
        public async Task ManifestTruncatedFileThrows()
        {
            var folder = CreateTempFolder();
            try
            {
                var manager = new ManifestManager(folder);
                var data = new ManifestData();
                data.AddFile(1, 0);
                await manager.WriteAsync(data);

                // Truncate the file to just 8 bytes (too small)
                var path = Path.Combine(folder, "manifest.mf");
                var bytes = File.ReadAllBytes(path);
                File.WriteAllBytes(path, bytes[..8]);

                Assert.Throws<InvalidDataException>(() => manager.TryRead());
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Regression")]
        public async Task ManifestOverwriteReplacesOldData()
        {
            var folder = CreateTempFolder();
            try
            {
                var manager = new ManifestManager(folder);

                var data1 = new ManifestData();
                data1.AddFile(1, 0);
                data1.AddFile(1, 1);
                await manager.WriteAsync(data1);

                // Overwrite with different data
                var data2 = new ManifestData();
                data2.AddFile(1, 5);
                data2.AddFile(2, 10);
                await manager.WriteAsync(data2);

                var readBack = manager.TryRead();
                Assert.NotNull(readBack);
                Assert.Equal(new[] { 5 }, readBack!.GetFiles(1).ToArray());
                Assert.Equal(new[] { 10 }, readBack.GetFiles(2).ToArray());
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Regression")]
        public async Task CleanupTempFileRemovesLeftoverTmp()
        {
            var folder = CreateTempFolder();
            try
            {
                // Simulate a crash that left a .tmp file
                var tmpPath = Path.Combine(folder, "manifest.mf.tmp");
                await File.WriteAllTextAsync(tmpPath, "leftover");

                var manager = new ManifestManager(folder);
                Assert.True(File.Exists(tmpPath));

                manager.CleanupTempFile();
                Assert.False(File.Exists(tmpPath));
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        // --- Integration tests: manifest + TrimDatabase ---

        [Fact]
        [Trait("Category", "Specification")]
        public async Task FirstRunCreatesManifestFile()
        {
            var folder = CreateTempFolder();
            try
            {
                var db = new TrimDatabase(MakeOptions(folder));
                await db.LoadAsync();
                await db.DisposeAsync();

                Assert.True(File.Exists(Path.Combine(folder, "manifest.mf")));
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Specification")]
        public async Task ManifestSurvivesReopenWithData()
        {
            var folder = CreateTempFolder();
            try
            {
                // Session 1: write data and close
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    for (var i = 0; i < 50; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"mf_key_{i:D4}");
                        var value = Encoding.UTF8.GetBytes($"mf_val_{i:D4}");
                        await db.PutAsync(key, value);
                    }

                    await db.DisposeAsync();
                }

                Assert.True(File.Exists(Path.Combine(folder, "manifest.mf")));

                // Session 2: reopen and verify data survives
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    for (var i = 0; i < 50; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"mf_key_{i:D4}");
                        var expected = Encoding.UTF8.GetBytes($"mf_val_{i:D4}");
                        var result = await db.GetAsync(key);
                        Assert.Equal(expected, result.ToArray());
                    }

                    await db.DisposeAsync();
                }
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Specification")]
        public async Task OrphanedFileDeletedOnRestart()
        {
            var folder = CreateTempFolder();
            try
            {
                // Session 1: write data to create SSTables, close cleanly
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    var words = CommonData.Words;
                    foreach (var word in words)
                    {
                        var key = Encoding.UTF8.GetBytes(word);
                        var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                        await db.PutAsync(key, value);
                    }

                    await db.DisposeAsync();
                }

                // Verify manifest and SSTables exist
                Assert.True(File.Exists(Path.Combine(folder, "manifest.mf")));
                var filesBeforeOrphan = Directory.GetFiles(folder, "*.trim");
                Assert.NotEmpty(filesBeforeOrphan);

                // Simulate a crashed compaction: drop a stray SSTable that isn't in the manifest
                var orphanPath = Path.Combine(folder, "Level1_999.trim");
                await File.WriteAllBytesAsync(orphanPath, new byte[4096]);
                Assert.True(File.Exists(orphanPath));

                // Session 2: reopen — manifest should cause orphan to be deleted
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    Assert.False(File.Exists(orphanPath), "Orphaned file should have been deleted on startup");

                    // Original data should still be intact
                    var words = CommonData.Words;
                    var probeKey = Encoding.UTF8.GetBytes(words[0]);
                    var result = await db.GetAsync(probeKey);
                    Assert.True(result.Length > 0, "Original data should survive orphan cleanup");

                    await db.DisposeAsync();
                }
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Specification")]
        public async Task ManifestReflectsFlushState()
        {
            var folder = CreateTempFolder();
            try
            {
                // Write enough data to trigger flush, then close
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    var words = CommonData.Words;
                    foreach (var word in words)
                    {
                        var key = Encoding.UTF8.GetBytes(word);
                        var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                        await db.PutAsync(key, value);
                    }

                    await db.DisposeAsync();
                }

                // Read the manifest directly and verify it lists some files
                var manager = new ManifestManager(folder);
                var data = manager.TryRead();
                Assert.NotNull(data);

                // Level 1 should have at least one file after flush
                var level1Files = data!.GetFiles(1);
                Assert.True(level1Files.Count > 0, "Manifest should list Level 1 files after flush");

                // Each file listed in manifest should exist on disk
                foreach (var fileIndex in level1Files)
                {
                    var filePath = Path.Combine(folder, $"Level1_{fileIndex}.trim");
                    Assert.True(File.Exists(filePath), $"Manifest lists Level1_{fileIndex} but file does not exist");
                }
            }
            finally
            {
                CleanupFolder(folder);
            }
        }

        [Fact]
        [Trait("Category", "Regression")]
        public async Task DisableManifestDoesNotCreateFile()
        {
            var folder = CreateTempFolder();
            try
            {
                var db = new TrimDatabase(MakeOptions(folder, disableManifest: true));
                await db.LoadAsync();

                for (var i = 0; i < 10; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key_{i}");
                    var value = Encoding.UTF8.GetBytes($"val_{i}");
                    await db.PutAsync(key, value);
                }

                await db.DisposeAsync();

                Assert.False(File.Exists(Path.Combine(folder, "manifest.mf")),
                    "Manifest file should not exist when DisableManifest is true");
            }
            finally
            {
                CleanupFolder(folder);
            }
        }
    }
}
