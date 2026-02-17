using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class ConcurrencyFacts
    {
        /// <summary>
        /// Test #53: 4 threads each put 1000 unique keys (no overlap between threads).
        /// After all complete, all 4000 keys retrievable from skip list.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void ConcurrentPutsDoNotLoseKeys()
        {
            const int threadCount = 4;
            const int keysPerThread = 1000;

            using var allocator = new ArrayBasedAllocator32(4096 * 50_000, 25);
            var skipList = new SkipList32(allocator);

            var tasks = new Task[threadCount];
            for (int t = 0; t < threadCount; t++)
            {
                var threadId = t;
                tasks[t] = Task.Run(() =>
                {
                    for (int i = 0; i < keysPerThread; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"t{threadId}-key-{i:D4}");
                        var value = Encoding.UTF8.GetBytes($"t{threadId}-val-{i:D4}");
                        var result = skipList.Put(key, value);
                        Assert.True(result, $"Put failed for thread {threadId}, key {i}");
                    }
                });
            }

            Task.WaitAll(tasks);

            // Verify all keys are retrievable
            for (int t = 0; t < threadCount; t++)
            {
                for (int i = 0; i < keysPerThread; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"t{t}-key-{i:D4}");
                    var result = skipList.TryGet(key, out _);
                    Assert.Equal(SearchResult.Found, result);
                }
            }
        }

        /// <summary>
        /// Test #54: Writer thread puts keys continuously. Reader thread gets keys.
        /// Reader never sees corrupted data -- may see NotFound for keys not yet
        /// written, but never garbage bytes.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void ConcurrentPutAndGet()
        {
            const int keyCount = 2000;

            using var allocator = new ArrayBasedAllocator32(4096 * 50_000, 25);
            var skipList = new SkipList32(allocator);

            var writerDone = new ManualResetEventSlim(false);

            var writerTask = Task.Run(() =>
            {
                for (int i = 0; i < keyCount; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key-{i:D5}");
                    var value = Encoding.UTF8.GetBytes($"value-{i:D5}");
                    skipList.Put(key, value);
                }
                writerDone.Set();
            });

            var readerTask = Task.Run(() =>
            {
                var rng = new Random(42);
                while (!writerDone.IsSet)
                {
                    var idx = rng.Next(keyCount);
                    var key = Encoding.UTF8.GetBytes($"key-{idx:D5}");
                    var expectedValue = Encoding.UTF8.GetBytes($"value-{idx:D5}");
                    var result = skipList.TryGet(key, out var value);

                    // NotFound is fine -- the writer may not have written this key yet.
                    // But if Found, the value must be correct, not garbage.
                    if (result == SearchResult.Found)
                    {
                        Assert.Equal(expectedValue, value.ToArray());
                    }
                }
            });

            Task.WaitAll(writerTask, readerTask);
        }

        /// <summary>
        /// Test #55: One thread puts keys, another deletes them. After both finish,
        /// every key is either found with correct value or deleted. No other state.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void ConcurrentPutAndDelete()
        {
            const int keyCount = 1000;

            using var allocator = new ArrayBasedAllocator32(4096 * 50_000, 25);
            var skipList = new SkipList32(allocator);

            // Pre-populate so deleter has something to delete
            for (int i = 0; i < keyCount; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{i:D4}");
                var value = Encoding.UTF8.GetBytes($"val-{i:D4}");
                skipList.Put(key, value);
            }

            var barrier = new CountdownEvent(2);

            var putTask = Task.Run(() =>
            {
                barrier.Signal();
                barrier.Wait();
                for (int i = 0; i < keyCount; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key-{i:D4}");
                    var value = Encoding.UTF8.GetBytes($"new-{i:D4}");
                    skipList.Put(key, value);
                }
            });

            var deleteTask = Task.Run(() =>
            {
                barrier.Signal();
                barrier.Wait();
                for (int i = 0; i < keyCount; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key-{i:D4}");
                    skipList.Delete(key);
                }
            });

            Task.WaitAll(putTask, deleteTask);

            // Every key must be either Found or Deleted. Never NotFound, never garbage.
            for (int i = 0; i < keyCount; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{i:D4}");
                var result = skipList.TryGet(key, out var value);

                Assert.True(
                    result == SearchResult.Found || result == SearchResult.Deleted,
                    $"Key {i}: expected Found or Deleted, got {result}");

                // If found, value must be one of the two known values
                if (result == SearchResult.Found)
                {
                    var originalValue = Encoding.UTF8.GetBytes($"val-{i:D4}");
                    var newValue = Encoding.UTF8.GetBytes($"new-{i:D4}");
                    var actual = value.ToArray();
                    Assert.True(
                        actual.AsSpan().SequenceEqual(originalValue) ||
                        actual.AsSpan().SequenceEqual(newValue),
                        $"Key {i}: value is garbage, not one of the expected values");
                }
            }
        }

        /// <summary>
        /// Test #56: Multiple writers active when memtable overflows. All writes must
        /// eventually succeed. No writer should block forever. The SwitchInMemoryTable
        /// semaphore ensures only one switch happens.
        /// Requires working flush path -- expected to fail today.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ConcurrentPutsDuringMemtableSwitch()
        {
            var folder = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "TrimDB_ConcSwitch_" + Guid.NewGuid().ToString("N"));

            try
            {
                var options = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => new MMapBlockCache(),
                    DisableMerging = true,
                    DisableWAL = true,
                    // Small allocator to force overflow quickly
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 100, 25))
                };

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                const int writerCount = 4;
                const int keysPerWriter = 500;
                var tasks = new Task[writerCount];

                for (int w = 0; w < writerCount; w++)
                {
                    var writerId = w;
                    tasks[w] = Task.Run(async () =>
                    {
                        for (int i = 0; i < keysPerWriter; i++)
                        {
                            var key = Encoding.UTF8.GetBytes($"w{writerId}-k{i:D4}");
                            var value = Encoding.UTF8.GetBytes($"w{writerId}-v{i:D4}");
                            await db.PutAsync(key, value);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                // Give flush a moment to settle
                await Task.Delay(500);

                // Verify all keys exist
                for (int w = 0; w < writerCount; w++)
                {
                    for (int i = 0; i < keysPerWriter; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"w{w}-k{i:D4}");
                        var result = await db.GetAsync(key);
                        Assert.True(result.Length > 0,
                            $"Key w{w}-k{i:D4} not found after concurrent writes");
                    }
                }

                await db.DisposeAsync();
            }
            finally
            {
                if (System.IO.Directory.Exists(folder))
                    System.IO.Directory.Delete(folder, true);
            }
        }

        /// <summary>
        /// Test #57: Fill memtable, begin flush (memtable moves to _oldInMemoryTables).
        /// Concurrent reads must find all keys -- either in old memtable or new SSTable.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ConcurrentReadsWorkDuringMemtableFlush()
        {
            var folder = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "TrimDB_ConcFlush_" + Guid.NewGuid().ToString("N"));

            try
            {
                var options = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => new MMapBlockCache(),
                    DisableMerging = true,
                    DisableWAL = true,
                    // Small allocator to trigger overflow
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 200, 25))
                };

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                // Write some known keys that will be in the first memtable
                var knownKeys = new List<byte[]>();
                for (int i = 0; i < 50; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"flush-key-{i:D4}");
                    var value = Encoding.UTF8.GetBytes($"flush-val-{i:D4}");
                    await db.PutAsync(key, value);
                    knownKeys.Add(key);
                }

                // Now trigger overflow by writing more data to force memtable switch
                var overflowTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 500; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"overflow-{i:D5}");
                        var value = Encoding.UTF8.GetBytes($"ovf-val-{i:D5}");
                        await db.PutAsync(key, value);
                    }
                });

                // Concurrently read the known keys -- they should always be findable
                var readTask = Task.Run(async () =>
                {
                    for (int round = 0; round < 10; round++)
                    {
                        foreach (var key in knownKeys)
                        {
                            var result = await db.GetAsync(key);
                            Assert.True(result.Length > 0,
                                $"Key {Encoding.UTF8.GetString(key)} not found during flush");
                        }
                    }
                });

                await Task.WhenAll(overflowTask, readTask);
                await db.DisposeAsync();
            }
            finally
            {
                if (System.IO.Directory.Exists(folder))
                    System.IO.Directory.Delete(folder, true);
            }
        }

        /// <summary>
        /// Test #58: The writer count barrier ensures that after all Put operations
        /// complete (decrement _currentWriters), the skip list can be safely iterated.
        /// We verify this by running concurrent Puts, waiting for them to finish, then
        /// immediately enumerating the skip list -- if the barrier is broken, we would
        /// see torn reads or missing keys.
        /// WaitForAbilityToWriteToDisk is internal, so we test its observable effect:
        /// after all writers return, enumeration is consistent.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void WriterCountBarrierWorks()
        {
            const int writerCount = 8;
            const int keysPerWriter = 200;

            using var allocator = new ArrayBasedAllocator32(4096 * 50_000, 25);
            var skipList = new SkipList32(allocator);

            var barrier = new CountdownEvent(writerCount);
            var writerTasks = new Task[writerCount];

            for (int w = 0; w < writerCount; w++)
            {
                var writerId = w;
                writerTasks[w] = Task.Run(() =>
                {
                    barrier.Signal();
                    barrier.Wait();
                    for (int i = 0; i < keysPerWriter; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"b{writerId}-{i:D4}");
                        var value = Encoding.UTF8.GetBytes($"v{writerId}-{i:D4}");
                        skipList.Put(key, value);
                    }
                });
            }

            Task.WaitAll(writerTasks);

            // After all writers return, _currentWriters must be 0.
            // Verify by enumerating the skip list -- all keys must be present and
            // the enumeration must not crash or hang.
            var enumeratedCount = 0;
            using var enumerator = skipList.GetEnumerator();
            while (enumerator.MoveNext())
            {
                enumeratedCount++;
                // Verify key is not empty/corrupt
                Assert.True(enumerator.Current.Key.Length > 0);
            }

            Assert.Equal(writerCount * keysPerWriter, enumeratedCount);
        }
    }
}
