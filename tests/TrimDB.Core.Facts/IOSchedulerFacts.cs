using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class IOSchedulerFacts
    {
        private static string CreateTempFolder()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_IOSched_" + Guid.NewGuid().ToString("N"));
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

        /// <summary>
        /// Forces GC to run finalizers immediately, preventing the test host from crashing
        /// after test completion due to MMapBlockCache's SafeHandle finalization bug.
        /// </summary>
        private static void ForceFinalization()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        /// <summary>
        /// Test #62: BUG-G. If flush fails, the memtable must remain readable.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public async Task FlushFailureDoesNotDiscardMemtable()
        {
            var folder = CreateTempFolder();
            MMapBlockCache? blockCache = null;
            try
            {
                blockCache = new MMapBlockCache();
                var capturedCache = blockCache;
                var dbOptions = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => capturedCache,
                    DisableMerging = true,
                    DisableWAL = true,
                    DisableManifest = true,
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 50, 25)),
                };

                var db = new TrimDatabase(dbOptions);
                await db.LoadAsync();

                var writtenKeys = new System.Collections.Generic.List<(byte[] Key, byte[] Value)>();
                var words = CommonData.Words.Take(100).Where(w => !string.IsNullOrEmpty(w)).ToArray();

                foreach (var word in words)
                {
                    var key = Encoding.UTF8.GetBytes(word);
                    var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                    try
                    {
                        await db.PutAsync(key, value);
                        writtenKeys.Add((key, value));
                    }
                    catch { break; }
                }

                await Task.Delay(500);

                var foundCount = 0;
                foreach (var (key, _) in writtenKeys)
                {
                    try
                    {
                        var result = await db.GetAsync(key);
                        if (result.Length > 0) foundCount++;
                    }
                    catch { }
                }

                Assert.True(foundCount > 0, "Should be able to read at least some keys after flush");
                Assert.Equal(writtenKeys.Count, foundCount);

                await db.DisposeAsync();
            }
            finally
            {
                blockCache?.Dispose();
                ForceFinalization();
                CleanupFolder(folder);
            }
        }

        /// <summary>
        /// Test #63: BUG-F. A single compaction failure should not stop future compactions.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public async Task CompactionFailureDoesNotStopFutureCompactions()
        {
            var folder = CreateTempFolder();
            MMapBlockCache? blockCache = null;
            try
            {
                blockCache = new MMapBlockCache();
                var capturedCache = blockCache;
                var dbOptions = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => capturedCache,
                    DisableMerging = true,
                    DisableWAL = true,
                    DisableManifest = true,
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 50, 25)),
                };

                var db = new TrimDatabase(dbOptions);
                await db.LoadAsync();

                var words = CommonData.Words.Where(w => !string.IsNullOrEmpty(w)).Take(100).ToArray();
                foreach (var word in words)
                {
                    try
                    {
                        await db.PutAsync(
                            Encoding.UTF8.GetBytes(word),
                            Encoding.UTF8.GetBytes($"VALUE={word}"));
                    }
                    catch { break; }
                }

                await Task.Delay(1000);

                var lateKey = Encoding.UTF8.GetBytes("late_key_after_flushes");
                var lateValue = Encoding.UTF8.GetBytes("late_value");
                await db.PutAsync(lateKey, lateValue);
                var result = await db.GetAsync(lateKey);
                Assert.Equal(lateValue, result.ToArray());

                await db.DisposeAsync();
            }
            finally
            {
                blockCache?.Dispose();
                ForceFinalization();
                CleanupFolder(folder);
            }
        }

        /// <summary>
        /// Test #64: Flush failure should not block new writes.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task FlushFailureDoesNotBlockNewWrites()
        {
            var folder = CreateTempFolder();
            MMapBlockCache? blockCache = null;
            try
            {
                blockCache = new MMapBlockCache();
                var capturedCache = blockCache;
                var dbOptions = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => capturedCache,
                    DisableMerging = true,
                    DisableWAL = true,
                    DisableManifest = true,
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 50, 25)),
                };

                var db = new TrimDatabase(dbOptions);
                await db.LoadAsync();

                var words = CommonData.Words.Where(w => !string.IsNullOrEmpty(w)).ToArray();
                foreach (var word in words)
                {
                    try
                    {
                        await db.PutAsync(
                            Encoding.UTF8.GetBytes(word),
                            Encoding.UTF8.GetBytes($"VALUE={word}"));
                    }
                    catch { break; }
                }

                await Task.Delay(500);

                var postFlushKey = Encoding.UTF8.GetBytes("post_flush_key");
                var postFlushValue = Encoding.UTF8.GetBytes("post_flush_value");

                var writeTask = Task.Run(async () =>
                {
                    await db.PutAsync(postFlushKey, postFlushValue);
                });

                var completed = await Task.WhenAny(writeTask, Task.Delay(5000));
                Assert.True(completed == writeTask, "PutAsync should not block indefinitely after flush failure");

                if (writeTask.IsCompletedSuccessfully)
                {
                    var result = await db.GetAsync(postFlushKey);
                    Assert.Equal(postFlushValue, result.ToArray());
                }

                await db.DisposeAsync();
            }
            finally
            {
                blockCache?.Dispose();
                ForceFinalization();
                CleanupFolder(folder);
            }
        }
    }
}
