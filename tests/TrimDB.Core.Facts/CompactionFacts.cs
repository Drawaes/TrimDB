using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrimDB.Core;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using TrimDB.Core.Storage.Layers;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class CompactionFacts : IDisposable
    {
        private readonly string _tempDir;
        private readonly MMapBlockCache _blockCache;
        private readonly MurmurHash3 _hasher = new();

        public CompactionFacts()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "TrimDB_Compact_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_tempDir);
            _blockCache = new MMapBlockCache();
        }

        public void Dispose()
        {
            _blockCache.Dispose();
            if (Directory.Exists(_tempDir))
            {
                try { Directory.Delete(_tempDir, true); } catch { }
            }
        }

        /// <summary>
        /// Test #35: Merger produces no duplicate keys.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task MergerProducesNoDuplicateKeys()
        {
            // Two streams with overlapping keys
            var stream1 = CreateAsyncStream(
                Enumerable.Range(0, 50).Select(i => (byte)(i * 2)).ToArray()); // 0,2,4,...,98
            var stream2 = CreateAsyncStream(
                Enumerable.Range(0, 50).Select(i => (byte)(i * 2 + 1)).ToArray()); // 1,3,5,...,99
            // Add some overlap
            var stream3 = CreateAsyncStream(new byte[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 });

            var merger = new TableFileMerger(new[]
            {
                stream1.GetAsyncEnumerator(),
                stream2.GetAsyncEnumerator(),
                stream3.GetAsyncEnumerator(),
            });

            var keys = new List<byte>();
            while (await merger.MoveNextAsync())
            {
                keys.Add(merger.Current.Key[0]);
            }

            // No duplicates
            Assert.Equal(keys.Count, keys.Distinct().Count());
            Assert.Equal(100, keys.Count);
        }

        /// <summary>
        /// Test #36: Merger output is strictly sorted.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task MergerOutputIsStrictlySorted()
        {
            var stream1 = CreateAsyncStream(new byte[] { 1, 5, 10, 20, 50 });
            var stream2 = CreateAsyncStream(new byte[] { 3, 7, 10, 15, 25 });
            var stream3 = CreateAsyncStream(new byte[] { 2, 5, 8, 12, 30, 100 });

            var merger = new TableFileMerger(new[]
            {
                stream1.GetAsyncEnumerator(),
                stream2.GetAsyncEnumerator(),
                stream3.GetAsyncEnumerator(),
            });

            var keys = new List<byte>();
            while (await merger.MoveNextAsync())
            {
                keys.Add(merger.Current.Key[0]);
            }

            for (var i = 1; i < keys.Count; i++)
            {
                Assert.True(keys[i - 1] < keys[i],
                    $"Output not sorted at index {i}: {keys[i - 1]} >= {keys[i]}");
            }
        }

        /// <summary>
        /// Test #37: Merger preserves the value from the first (newest) stream for duplicate keys.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MergerPreservesNewestValueForDuplicateKeys()
        {
            // Stream1 is "newest" (index 0), stream2 is "oldest" (index 1)
            var stream1 = CreateAsyncStreamWithValues(new[]
            {
                ((byte)10, (byte)0xAA),
                ((byte)20, (byte)0xBB),
            });
            var stream2 = CreateAsyncStreamWithValues(new[]
            {
                ((byte)10, (byte)0x11),
                ((byte)20, (byte)0x22),
                ((byte)30, (byte)0x33),
            });

            var merger = new TableFileMerger(new[]
            {
                stream1.GetAsyncEnumerator(),
                stream2.GetAsyncEnumerator(),
            });

            var results = new Dictionary<byte, byte>();
            while (await merger.MoveNextAsync())
            {
                results[merger.Current.Key[0]] = merger.Current.Value[0];
            }

            // Key 10 and 20: stream1 (newest) value wins
            Assert.Equal(0xAA, results[10]);
            Assert.Equal(0xBB, results[20]);
            // Key 30: only in stream2
            Assert.Equal(0x33, results[30]);
        }

        /// <summary>
        /// Test #38: Merge preserves tombstones from the newer stream.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MergePreservesTombstones()
        {
            // Newer stream has a tombstone for key 10
            var newerChannel = Channel.CreateUnbounded<IMemoryItem>();
            newerChannel.Writer.TryWrite(new TestMemoryItem(10, 0, isDeleted: true));
            newerChannel.Writer.TryWrite(new TestMemoryItem(20, 0xAA));
            newerChannel.Writer.Complete();

            // Older stream has live value for key 10
            var olderChannel = Channel.CreateUnbounded<IMemoryItem>();
            olderChannel.Writer.TryWrite(new TestMemoryItem(10, 0xFF));
            olderChannel.Writer.TryWrite(new TestMemoryItem(30, 0xCC));
            olderChannel.Writer.Complete();

            var merger = new TableFileMerger(new[]
            {
                newerChannel.Reader.ReadAllAsync().GetAsyncEnumerator(),
                olderChannel.Reader.ReadAllAsync().GetAsyncEnumerator(),
            });

            IMemoryItem? key10Item = null;
            while (await merger.MoveNextAsync())
            {
                if (merger.Current.Key[0] == 10)
                {
                    // Capture the current state before MoveNext advances
                    key10Item = new SnapshotMemoryItem(merger.Current);
                }
            }

            Assert.NotNull(key10Item);
            Assert.True(key10Item!.IsDeleted, "Tombstone from newer stream should win");
        }

        /// <summary>
        /// Test #39: End-to-end compaction writes a valid SSTable.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task CompactionWritesValidSSTable()
        {
            // Create two SSTables
            var file1 = Path.Combine(_tempDir, "Level2_0.trim");
            var file2 = Path.Combine(_tempDir, "Level2_1.trim");

            var pairs1 = Enumerable.Range(0, 100)
                .Select(i => (Encoding.UTF8.GetBytes($"a{i:D4}"), Encoding.UTF8.GetBytes($"val1_{i:D4}")))
                .ToArray();

            var pairs2 = Enumerable.Range(0, 100)
                .Select(i => (Encoding.UTF8.GetBytes($"b{i:D4}"), Encoding.UTF8.GetBytes($"val2_{i:D4}")))
                .ToArray();

            await WriteSSTable(file1, pairs1);
            await WriteSSTable(file2, pairs2);

            var table1 = new TableFile(file1, _blockCache);
            var table2 = new TableFile(file2, _blockCache);
            await table1.LoadAsync();
            await table2.LoadAsync();

            // Create a sorted layer for the merge writer to get filenames from
            var outputDir = Path.Combine(_tempDir, "output");
            Directory.CreateDirectory(outputDir);
            var outputLayer = new SortedStorageLayer(3, outputDir, _blockCache, 1024 * 1024 * 16, 10);

            // Merge
            await using var merger = new TableFileMerger(new[]
            {
                table1.GetAsyncEnumerator(),
                table2.GetAsyncEnumerator(),
            });

            var mergeWriter = new TableFileMergeWriter(outputLayer, _blockCache);
            await mergeWriter.WriteFromMerger(merger);

            // Verify the output SSTable(s) contain all keys
            var allKeys = pairs1.Concat(pairs2)
                .Select(p => Encoding.UTF8.GetString(p.Item1))
                .OrderBy(k => k)
                .ToList();

            var readKeys = new List<string>();
            foreach (var outputTable in mergeWriter.NewTableFiles)
            {
                await foreach (var item in outputTable)
                {
                    readKeys.Add(Encoding.UTF8.GetString(item.Key));
                }
            }

            readKeys.Sort(StringComparer.Ordinal);
            Assert.Equal(allKeys, readKeys);
        }

        /// <summary>
        /// Test #40: Compaction under concurrent reads.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task CompactionDoesNotCorruptUnderConcurrentReads()
        {
            // Create an SSTable with known data
            var file1 = Path.Combine(_tempDir, "Level2_0.trim");
            var pairs = Enumerable.Range(0, 200)
                .Select(i => (Encoding.UTF8.GetBytes($"key{i:D4}"), Encoding.UTF8.GetBytes($"val{i:D4}")))
                .ToArray();

            await WriteSSTable(file1, pairs);

            var table = new TableFile(file1, _blockCache);
            await table.LoadAsync();

            // Run reads concurrently while iterating (simulating compaction read)
            var readTask = Task.Run(async () =>
            {
                for (var round = 0; round < 10; round++)
                {
                    foreach (var (key, _) in pairs.Take(50))
                    {
                        var hash = _hasher.ComputeHash64(key);
                        var result = await table.GetAsync(key, hash);
                        // Should be Found or NotFound, never garbage
                        Assert.True(result.Result == SearchResult.Found || result.Result == SearchResult.NotFound,
                            $"Unexpected result: {result.Result}");
                    }
                }
            });

            var iterateTask = Task.Run(async () =>
            {
                for (var round = 0; round < 5; round++)
                {
                    await foreach (var item in table)
                    {
                        // Just consume -- verifying no crash
                        _ = item.Key.Length;
                    }
                }
            });

            await Task.WhenAll(readTask, iterateTask);
        }

        // --- Helpers ---

        private async Task WriteSSTable(string fileName, (byte[] Key, byte[] Value)[] pairs)
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 10_000, 25);
            var skipList = new SkipList32(allocator);
            foreach (var (key, value) in pairs)
            {
                skipList.Put(key, value);
            }
            var writer = new TableFileWriter(fileName);
            await writer.SaveMemoryTable(skipList);
        }

        private static IAsyncEnumerable<IMemoryItem> CreateAsyncStream(byte[] keys)
        {
            var channel = Channel.CreateUnbounded<IMemoryItem>();
            foreach (var k in keys)
            {
                channel.Writer.TryWrite(new TestMemoryItem(k, k));
            }
            channel.Writer.Complete();
            return channel.Reader.ReadAllAsync();
        }

        private static IAsyncEnumerable<IMemoryItem> CreateAsyncStreamWithValues((byte Key, byte Value)[] items)
        {
            var channel = Channel.CreateUnbounded<IMemoryItem>();
            foreach (var (k, v) in items)
            {
                channel.Writer.TryWrite(new TestMemoryItem(k, v));
            }
            channel.Writer.Complete();
            return channel.Reader.ReadAllAsync();
        }

        internal class TestMemoryItem : IMemoryItem
        {
            private readonly byte[] _key;
            private readonly byte[] _value;
            private readonly bool _isDeleted;

            public TestMemoryItem(byte keyNumber, byte valueNumber, bool isDeleted = false)
            {
                _key = new[] { keyNumber };
                _value = new[] { valueNumber };
                _isDeleted = isDeleted;
            }

            public ReadOnlySpan<byte> Key => _key;
            public ReadOnlySpan<byte> Value => _value;
            public bool IsDeleted => _isDeleted;
        }

        /// <summary>
        /// Captures a snapshot of an IMemoryItem (whose Key/Value are Spans that may be invalidated).
        /// </summary>
        internal class SnapshotMemoryItem : IMemoryItem
        {
            private readonly byte[] _key;
            private readonly byte[] _value;
            private readonly bool _isDeleted;

            public SnapshotMemoryItem(IMemoryItem source)
            {
                _key = source.Key.ToArray();
                _value = source.Value.ToArray();
                _isDeleted = source.IsDeleted;
            }

            public ReadOnlySpan<byte> Key => _key;
            public ReadOnlySpan<byte> Value => _value;
            public bool IsDeleted => _isDeleted;
        }
    }
}
