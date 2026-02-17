using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class SSTableRoundTripFacts : IDisposable
    {
        private readonly string _tempDir;
        private readonly MMapBlockCache _blockCache;
        private readonly MurmurHash3 _hasher = new();

        public SSTableRoundTripFacts()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "TrimDB_SSTable_" + Guid.NewGuid().ToString("N"));
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
        /// Test #23: Write a single key via TableFileWriter, read it back via TableFile.GetAsync.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task WriteAndReadSingleKey()
        {
            var key = Encoding.UTF8.GetBytes("hello");
            var value = Encoding.UTF8.GetBytes("world");

            var fileName = Path.Combine(_tempDir, "Level1_0.trim");
            var (skipList, allocator) = CreateSkipListWith(new[] { (key, value) });
            using (allocator)
            {
                var writer = new TableFileWriter(fileName);
                await writer.SaveMemoryTable(skipList);
            }

            var table = new TableFile(fileName, _blockCache);
            await table.LoadAsync();

            var hash = _hasher.ComputeHash64(key);
            var result = await table.GetAsync(key, hash);

            Assert.Equal(SearchResult.Found, result.Result);
            Assert.Equal(value, result.Value.ToArray());
        }

        /// <summary>
        /// Test #24: Write 1000 keys, read all back with correct values.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task WriteAndReadManyKeys()
        {
            var pairs = new List<(byte[] Key, byte[] Value)>();
            for (var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D6}");
                var value = Encoding.UTF8.GetBytes($"value{i:D6}");
                pairs.Add((key, value));
            }

            var fileName = Path.Combine(_tempDir, "Level1_0.trim");
            var (skipList, allocator) = CreateSkipListWith(pairs.ToArray());
            using (allocator)
            {
                var writer = new TableFileWriter(fileName);
                await writer.SaveMemoryTable(skipList);
            }

            var table = new TableFile(fileName, _blockCache);
            await table.LoadAsync();

            foreach (var (key, value) in pairs)
            {
                var hash = _hasher.ComputeHash64(key);
                var result = await table.GetAsync(key, hash);
                Assert.Equal(SearchResult.Found, result.Result);
                Assert.Equal(value, result.Value.ToArray());
            }
        }

        /// <summary>
        /// Test #25: FirstKey and LastKey match expectations.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task SSTableFirstKeyLastKeyCorrect()
        {
            var pairs = new List<(byte[] Key, byte[] Value)>();
            for (var i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"val{i:D4}");
                pairs.Add((key, value));
            }

            var fileName = Path.Combine(_tempDir, "Level1_0.trim");
            var (skipList, allocator) = CreateSkipListWith(pairs.ToArray());
            using (allocator)
            {
                var writer = new TableFileWriter(fileName);
                await writer.SaveMemoryTable(skipList);
            }

            var table = new TableFile(fileName, _blockCache);
            await table.LoadAsync();

            // Keys are sorted lexicographically. "key0000" < "key0001" < ... < "key0099"
            var sortedKeys = pairs.Select(p => p.Key).OrderBy(k => k, ByteArrayComparer.Instance).ToList();
            Assert.Equal(sortedKeys.First(), table.FirstKey.ToArray());
            Assert.Equal(sortedKeys.Last(), table.LastKey.ToArray());
        }

        /// <summary>
        /// Test #26: Iterator yields all keys in sorted order with correct values.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task SSTableIteratorYieldsAllKeysInOrder()
        {
            var pairs = new List<(byte[] Key, byte[] Value)>();
            for (var i = 0; i < 200; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"VALUE=key{i:D4}");
                pairs.Add((key, value));
            }

            var fileName = Path.Combine(_tempDir, "Level1_0.trim");
            var (skipList, allocator) = CreateSkipListWith(pairs.ToArray());
            using (allocator)
            {
                var writer = new TableFileWriter(fileName);
                await writer.SaveMemoryTable(skipList);
            }

            var table = new TableFile(fileName, _blockCache);
            await table.LoadAsync();

            var readItems = new List<(string Key, string Value)>();
            await foreach (var item in table)
            {
                readItems.Add((Encoding.UTF8.GetString(item.Key), Encoding.UTF8.GetString(item.Value)));
            }

            Assert.Equal(200, readItems.Count);

            // Verify sorted order
            for (var i = 1; i < readItems.Count; i++)
            {
                Assert.True(string.Compare(readItems[i - 1].Key, readItems[i].Key, StringComparison.Ordinal) < 0,
                    $"Keys not sorted: {readItems[i - 1].Key} >= {readItems[i].Key}");
            }

            // Verify values
            foreach (var (key, value) in readItems)
            {
                Assert.Equal($"VALUE={key}", value);
            }
        }

        /// <summary>
        /// Test #27: Single key SSTable edge case.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task SingleKeySSTable()
        {
            var key = Encoding.UTF8.GetBytes("onlykey");
            var value = Encoding.UTF8.GetBytes("onlyvalue");

            var fileName = Path.Combine(_tempDir, "Level1_0.trim");
            var (skipList, allocator) = CreateSkipListWith(new[] { (key, value) });
            using (allocator)
            {
                var writer = new TableFileWriter(fileName);
                await writer.SaveMemoryTable(skipList);
            }

            var table = new TableFile(fileName, _blockCache);
            await table.LoadAsync();

            Assert.Equal(key, table.FirstKey.ToArray());
            Assert.Equal(key, table.LastKey.ToArray());

            var hash = _hasher.ComputeHash64(key);
            var result = await table.GetAsync(key, hash);
            Assert.Equal(SearchResult.Found, result.Result);
            Assert.Equal(value, result.Value.ToArray());
        }

        /// <summary>
        /// Test #28: XorFilter rejects absent keys.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task XorFilterRejectsAbsentKeys()
        {
            var pairs = new List<(byte[] Key, byte[] Value)>();
            for (var i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"present{i:D4}");
                var value = Encoding.UTF8.GetBytes($"val{i:D4}");
                pairs.Add((key, value));
            }

            var fileName = Path.Combine(_tempDir, "Level1_0.trim");
            var (skipList, allocator) = CreateSkipListWith(pairs.ToArray());
            using (allocator)
            {
                var writer = new TableFileWriter(fileName);
                await writer.SaveMemoryTable(skipList);
            }

            var table = new TableFile(fileName, _blockCache);
            await table.LoadAsync();

            // Query 1000 absent keys. GetAsync must return NotFound for all of them.
            for (var i = 0; i < 1000; i++)
            {
                var absentKey = Encoding.UTF8.GetBytes($"absent{i:D6}");
                var hash = _hasher.ComputeHash64(absentKey);
                var result = await table.GetAsync(absentKey, hash);
                Assert.Equal(SearchResult.NotFound, result.Result);
            }
        }

        /// <summary>
        /// Test #29: BUG-C: _isDeleted stickiness. A tombstone followed by a live key
        /// in the same block must not mark the live key as deleted.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public void TombstoneFollowedByLiveKeyInSameBlock()
        {
            // Build a slotted block with a tombstone then a live entry using SlottedBlockBuilder.
            var block = new byte[FileConsts.PageSize];
            var builder = new TrimDB.Core.Storage.Blocks.SlottedBlockBuilder(block);

            var tombKey = Encoding.UTF8.GetBytes("delkey1");
            var liveKey = Encoding.UTF8.GetBytes("livekey2");
            var liveValue = Encoding.UTF8.GetBytes("livevalue");

            Assert.True(builder.TryAdd(tombKey, ReadOnlySpan<byte>.Empty, isDeleted: true));
            Assert.True(builder.TryAdd(liveKey, liveValue, isDeleted: false));
            builder.Finish();

            var reader = new BlockReader(new BlockEncodingFacts.ByteArrayMemoryOwner(block));

            // Read tombstone
            Assert.True(reader.TryGetNextKey(out var key1));
            Assert.Equal("delkey1", Encoding.UTF8.GetString(key1));
            Assert.True(reader.IsDeleted, "First entry should be marked as deleted");

            // Read live entry
            Assert.True(reader.TryGetNextKey(out var key2));
            Assert.Equal("livekey2", Encoding.UTF8.GetString(key2));

            Assert.False(reader.IsDeleted, "Live entry after tombstone should NOT be marked as deleted");

            var value = reader.GetCurrentValue();
            Assert.Equal("livevalue", Encoding.UTF8.GetString(value.Span));
        }

        /// <summary>
        /// Test #30: BUG-D: Tombstones written via memtable flush should be readable as deleted.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public async Task SSTableHandlesTombstoneEntries()
        {
            // Create a skip list with a key that is then deleted (tombstone)
            using var allocator = new ArrayBasedAllocator32(4096 * 1000, 25);
            var skipList = new SkipList32(allocator);

            var key1 = Encoding.UTF8.GetBytes("alive");
            var val1 = Encoding.UTF8.GetBytes("alivevalue");
            skipList.Put(key1, val1);

            var key2 = Encoding.UTF8.GetBytes("deadkey");
            var val2 = Encoding.UTF8.GetBytes("deadvalue");
            skipList.Put(key2, val2);
            skipList.Delete(key2);

            var fileName = Path.Combine(_tempDir, "Level1_0.trim");
            var writer = new TableFileWriter(fileName);
            await writer.SaveMemoryTable(skipList);

            var table = new TableFile(fileName, _blockCache);
            await table.LoadAsync();

            // Iterate and check that the tombstone is flagged as deleted
            var foundTombstone = false;
            await foreach (var item in table)
            {
                var k = Encoding.UTF8.GetString(item.Key);
                if (k == "deadkey")
                {
                    foundTombstone = true;
                    // BUG-D: BlockWriter writes value.Length (0) not -1, so IsDeleted will be false
                    Assert.True(item.IsDeleted, "Tombstone entry should have IsDeleted=true (BUG-D)");
                }
            }

            Assert.True(foundTombstone, "Tombstone entry should be present in SSTable iteration");
        }

        // --- Helpers ---

        private (SkipList32 SkipList, ArrayBasedAllocator32 Allocator) CreateSkipListWith(
            (byte[] Key, byte[] Value)[] pairs)
        {
            var allocator = new ArrayBasedAllocator32(4096 * 10_000, 25);
            var skipList = new SkipList32(allocator);

            foreach (var (key, value) in pairs)
            {
                skipList.Put(key, value);
            }

            return (skipList, allocator);
        }

        private class ByteArrayComparer : IComparer<byte[]>
        {
            public static readonly ByteArrayComparer Instance = new();

            public int Compare(byte[]? x, byte[]? y)
            {
                if (x is null && y is null) return 0;
                if (x is null) return -1;
                if (y is null) return 1;
                return x.AsSpan().SequenceCompareTo(y);
            }
        }
    }
}
