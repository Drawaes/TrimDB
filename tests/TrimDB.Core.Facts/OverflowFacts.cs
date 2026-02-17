using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using TrimDB.Core.Storage.Filters;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class OverflowFacts : IAsyncLifetime
    {
        private readonly string _folder;
        private TrimDatabase _db;

        public OverflowFacts()
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
                // Very small allocator: ~400KB to trigger overflow quickly
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

        // #48
        [Fact]
        [Trait("Category", "Specification")]
        public async Task MemtableOverflowTriggersFlush()
        {
            var words = CommonData.Words;

            // Write enough data to overflow the small allocator
            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await _db.PutAsync(key, value);
            }

            await Task.Delay(1000);

            // All previously written keys must still be findable
            // (in old memtable, new memtable, or on disk)
            foreach (var word in words)
            {
                var key = Encoding.UTF8.GetBytes(word);
                var result = await _db.GetAsync(key);
                Assert.True(result.Length > 0, $"Key '{word}' lost after overflow");
            }
        }

        // #49
        [Fact]
        [Trait("Category", "Regression")]
        public void AllocatorExhaustionReturnsFalse()
        {
            // Use an allocator independent of the database
            using var allocator = new ArrayBasedAllocator32(4096 * 10, 25); // ~40KB -- tiny
            var skipList = new SkipList32(allocator);

            var inserted = 0;
            for (var i = 0; i < 10000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key_{i:D6}");
                var value = Encoding.UTF8.GetBytes($"val_{i:D6}");
                var result = skipList.Put(key, value);
                if (!result) break;
                inserted++;
            }

            // Some keys must have been inserted before exhaustion
            Assert.True(inserted > 0, "No keys were inserted at all");
            // And at least one key must have been rejected (allocator full)
            Assert.True(inserted < 10000, "Allocator never reported full");

            // All previously inserted keys must still be retrievable
            for (var i = 0; i < inserted; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key_{i:D6}");
                var result = skipList.TryGet(key, out _);
                Assert.Equal(SearchResult.Found, result);
            }
        }

        // #50
        [Fact]
        [Trait("Category", "Regression")]
        public void AllocatorExhaustionDoesNotCorruptExistingData()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 10, 25);
            var skipList = new SkipList32(allocator);

            var insertedKeys = new List<(byte[] key, byte[] value)>();
            for (var i = 0; i < 10000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key_{i:D6}");
                var value = Encoding.UTF8.GetBytes($"val_{i:D6}");
                if (!skipList.Put(key, value)) break;
                insertedKeys.Add((key, value));
            }

            Assert.True(insertedKeys.Count > 0);

            // Verify every previously inserted key returns the correct value bytes
            foreach (var (key, expectedValue) in insertedKeys)
            {
                var result = skipList.TryGet(key, out var actualValue);
                Assert.Equal(SearchResult.Found, result);
                Assert.Equal(expectedValue, actualValue.ToArray());
            }
        }

        // #51 - BUG-B: BlockWriter infinite loop on oversized entry
        [Fact]
        [Trait("Category", "Bug")]
        public async Task OversizedEntryDoesNotInfiniteLoop()
        {
            // Create a single KV pair whose combined size exceeds FileConsts.PageSize (4096).
            // BlockWriter.WriteBlock should either succeed or indicate error, not hang.
            var oversizedKey = Encoding.UTF8.GetBytes("big_key");
            var oversizedValue = new byte[5000]; // Exceeds 4096 page size with headers
            new Random(42).NextBytes(oversizedValue);

            var item = new FakeMemoryItem(oversizedKey, oversizedValue, false);
            var iterator = new SingleItemEnumerator(item);
            iterator.MoveNext(); // Position on the first (only) element

            var filter = new NoOpFilter();
            using var blockWriter = new BlockWriter(iterator, filter);

            var block = new byte[4096];

            // Run WriteBlock with a timeout. If it hangs, the test fails with timeout instead
            // of blocking the test runner.
            var writeTask = Task.Run(() => blockWriter.WriteBlock(block));
            var completed = await Task.WhenAny(writeTask, Task.Delay(2000));

            Assert.True(completed == writeTask, "BlockWriter.WriteBlock hung on oversized entry (BUG-B infinite loop)");
        }

        // #52
        [Fact]
        [Trait("Category", "Bug")]
        public void DuplicateKeyFloodDoesNotExhaustAllocatorPrematurely()
        {
            // Inserting the same key 10,000 times with different values.
            // SetValueLocation updates the pointer but old values leak (append-only allocator).
            using var allocator = new ArrayBasedAllocator32(4096 * 1000, 25); // ~4MB
            var skipList = new SkipList32(allocator);

            var key = Encoding.UTF8.GetBytes("same_key");
            var successCount = 0;

            for (var i = 0; i < 10000; i++)
            {
                var value = Encoding.UTF8.GetBytes($"value_{i:D6}");
                if (!skipList.Put(key, value))
                {
                    break;
                }
                successCount++;
            }

            // With 4MB allocator and ~12 bytes per value, we should be able to fit many updates.
            // If the allocator exhausts before 10,000, it means old values are leaked.
            // The test documents this leak.
            // Even with leaking, 4MB / 18 bytes per value ~= 200K+ entries, so 10K should fit.
            Assert.True(successCount >= 10000,
                $"Allocator exhausted after only {successCount} updates of the same key. " +
                "Old value space is leaked (append-only allocator).");

            // Verify the final value is correct
            var result = skipList.TryGet(key, out var finalValue);
            Assert.Equal(SearchResult.Found, result);
            Assert.Equal(Encoding.UTF8.GetBytes($"value_{9999:D6}"), finalValue.ToArray());
        }

        // Helper types for the oversized entry test

        private sealed class FakeMemoryItem : IMemoryItem
        {
            private readonly byte[] _key;
            private readonly byte[] _value;
            private readonly bool _isDeleted;

            public FakeMemoryItem(byte[] key, byte[] value, bool isDeleted)
            {
                _key = key;
                _value = value;
                _isDeleted = isDeleted;
            }

            public ReadOnlySpan<byte> Key => _key;
            public ReadOnlySpan<byte> Value => _value;
            public bool IsDeleted => _isDeleted;
        }

        /// <summary>
        /// Enumerator that yields a single IMemoryItem, then stops.
        /// </summary>
        private sealed class SingleItemEnumerator : IEnumerator<IMemoryItem>
        {
            private readonly FakeMemoryItem _item;
            private bool _moved;

            public SingleItemEnumerator(FakeMemoryItem item) => _item = item;

            public IMemoryItem Current => _item;
            object IEnumerator.Current => Current;

            public bool MoveNext()
            {
                if (!_moved)
                {
                    _moved = true;
                    return true;
                }
                return false;
            }

            public void Reset() => _moved = false;
            public void Dispose() { }
        }

        /// <summary>
        /// A no-op filter for testing BlockWriter in isolation.
        /// </summary>
        private sealed class NoOpFilter : Filter
        {
            public override bool MayContainKey(long hashedValue) => true;
            public override bool AddKey(ReadOnlySpan<byte> key) => true;
            public override int WriteToPipe(System.IO.Pipelines.PipeWriter pipeWriter) => 0;
            public override void LoadFromBlock(ReadOnlyMemory<byte> memory) { }
        }
    }
}
