using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DurabilityFacts
    {
        private static TrimDatabaseOptions MakeOptions(string folder, int allocatorSize = 4096 * 100)
        {
            return new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(allocatorSize, 25))
            };
        }

        // #59
        [Fact]
        [Trait("Category", "Specification")]
        public async Task CloseAndReopenPreservesData()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                var keyCount = 50;

                // Phase 1: write data and close
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    for (var i = 0; i < keyCount; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"durable_key_{i:D4}");
                        var value = Encoding.UTF8.GetBytes($"durable_val_{i:D4}");
                        await db.PutAsync(key, value);
                    }

                    await db.DisposeAsync();
                }

                // Phase 2: reopen and verify
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    for (var i = 0; i < keyCount; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"durable_key_{i:D4}");
                        var expected = Encoding.UTF8.GetBytes($"durable_val_{i:D4}");
                        var result = await db.GetAsync(key);
                        Assert.Equal(expected, result.ToArray());
                    }

                    await db.DisposeAsync();
                }
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #60
        [Fact]
        [Trait("Category", "Specification")]
        public async Task CloseAndReopenPreservesMultipleLevels()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                // Phase 1: write enough data to trigger multiple flushes (multiple L1 SSTables)
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    var words = CommonData.Words;
                    // Write 3 batches to get multiple SSTables
                    for (var batch = 0; batch < 3; batch++)
                    {
                        foreach (var word in words)
                        {
                            var key = Encoding.UTF8.GetBytes($"b{batch}_{word}");
                            var value = Encoding.UTF8.GetBytes($"V_b{batch}_{word}");
                            await db.PutAsync(key, value);
                        }
                    }

                    await db.DisposeAsync();
                }

                // Phase 2: reopen and verify all batches
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    var words = CommonData.Words;
                    for (var batch = 0; batch < 3; batch++)
                    {
                        foreach (var word in words)
                        {
                            var key = Encoding.UTF8.GetBytes($"b{batch}_{word}");
                            var expected = Encoding.UTF8.GetBytes($"V_b{batch}_{word}");
                            var result = await db.GetAsync(key);
                            Assert.Equal(expected, result.ToArray());
                        }
                    }

                    await db.DisposeAsync();
                }
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #61
        [Fact]
        [Trait("Category", "Specification")]
        public async Task SSTFilesOnDiskMatchExpectedAfterReopen()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            try
            {
                // Phase 1: write data to create SSTables
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

                // Get files on disk before reopen
                var filesBeforeReopen = Directory.GetFiles(folder, "*.trim")
                    .Select(Path.GetFileName)
                    .OrderBy(f => f)
                    .ToArray();
                Assert.NotEmpty(filesBeforeReopen);

                // Phase 2: reopen and verify files survive and data is readable
                {
                    var db = new TrimDatabase(MakeOptions(folder));
                    await db.LoadAsync();

                    // Files on disk must be the same after reopen (no corruption or loss)
                    var filesAfterReopen = Directory.GetFiles(folder, "*.trim")
                        .Select(Path.GetFileName)
                        .OrderBy(f => f)
                        .ToArray();

                    Assert.Equal(filesBeforeReopen, filesAfterReopen);

                    // Verify data is still readable as a sanity check
                    var words = CommonData.Words;
                    var probeKey = Encoding.UTF8.GetBytes(words[0]);
                    var probeResult = await db.GetAsync(probeKey);
                    Assert.True(probeResult.Length > 0, "Data not readable after reopen");

                    await db.DisposeAsync();
                }
            }
            finally
            {
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }
    }
}
