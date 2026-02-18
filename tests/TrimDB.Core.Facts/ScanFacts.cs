using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class ScanFacts : DatabaseTestBase
    {
        // #50
        [Fact]
        [Trait("Category", "Specification")]
        public async Task FullScanReturnsAllKeysSorted()
        {
            var keys = new[] { "cherry", "apple", "banana", "date", "elderberry" };
            foreach (var k in keys)
            {
                await _db.PutAsync(Encoding.UTF8.GetBytes(k), Encoding.UTF8.GetBytes($"val_{k}"));
            }

            var results = new List<ScanEntry>();
            await foreach (var entry in _db.ScanAsync())
            {
                results.Add(entry);
            }

            Assert.Equal(keys.Length, results.Count);

            // Verify sorted order
            for (var i = 1; i < results.Count; i++)
            {
                var prev = results[i - 1].Key.Span;
                var curr = results[i].Key.Span;
                Assert.True(prev.SequenceCompareTo(curr) < 0,
                    $"Keys not sorted: '{Encoding.UTF8.GetString(prev)}' >= '{Encoding.UTF8.GetString(curr)}'");
            }
        }

        // #51
        [Fact]
        [Trait("Category", "Specification")]
        public async Task FullScanSkipsTombstones()
        {
            await _db.PutAsync(Encoding.UTF8.GetBytes("aaa"), Encoding.UTF8.GetBytes("v1"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("bbb"), Encoding.UTF8.GetBytes("v2"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ccc"), Encoding.UTF8.GetBytes("v3"));

            await _db.DeleteAsync(Encoding.UTF8.GetBytes("bbb"));

            var results = new List<ScanEntry>();
            await foreach (var entry in _db.ScanAsync())
            {
                results.Add(entry);
            }

            Assert.Equal(2, results.Count);
            Assert.Equal("aaa", Encoding.UTF8.GetString(results[0].Key.Span));
            Assert.Equal("ccc", Encoding.UTF8.GetString(results[1].Key.Span));
        }

        // #52
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ScanWithStartKeyIsInclusive()
        {
            await _db.PutAsync(Encoding.UTF8.GetBytes("aaa"), Encoding.UTF8.GetBytes("v1"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("bbb"), Encoding.UTF8.GetBytes("v2"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ccc"), Encoding.UTF8.GetBytes("v3"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ddd"), Encoding.UTF8.GetBytes("v4"));

            var results = new List<ScanEntry>();
            await foreach (var entry in _db.ScanAsync(Encoding.UTF8.GetBytes("bbb")))
            {
                results.Add(entry);
            }

            Assert.Equal(3, results.Count);
            Assert.Equal("bbb", Encoding.UTF8.GetString(results[0].Key.Span));
            Assert.Equal("ccc", Encoding.UTF8.GetString(results[1].Key.Span));
            Assert.Equal("ddd", Encoding.UTF8.GetString(results[2].Key.Span));
        }

        // #53
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ScanWithStartAndEndKeyIsInclusive()
        {
            await _db.PutAsync(Encoding.UTF8.GetBytes("aaa"), Encoding.UTF8.GetBytes("v1"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("bbb"), Encoding.UTF8.GetBytes("v2"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ccc"), Encoding.UTF8.GetBytes("v3"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ddd"), Encoding.UTF8.GetBytes("v4"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("eee"), Encoding.UTF8.GetBytes("v5"));

            var results = new List<ScanEntry>();
            await foreach (var entry in _db.ScanAsync(
                Encoding.UTF8.GetBytes("bbb"),
                Encoding.UTF8.GetBytes("ddd")))
            {
                results.Add(entry);
            }

            Assert.Equal(3, results.Count);
            Assert.Equal("bbb", Encoding.UTF8.GetString(results[0].Key.Span));
            Assert.Equal("ccc", Encoding.UTF8.GetString(results[1].Key.Span));
            Assert.Equal("ddd", Encoding.UTF8.GetString(results[2].Key.Span));
        }

        // #54
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ScanWithEndKeyStopsEarly()
        {
            await _db.PutAsync(Encoding.UTF8.GetBytes("aaa"), Encoding.UTF8.GetBytes("v1"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("bbb"), Encoding.UTF8.GetBytes("v2"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ccc"), Encoding.UTF8.GetBytes("v3"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ddd"), Encoding.UTF8.GetBytes("v4"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("eee"), Encoding.UTF8.GetBytes("v5"));

            var results = new List<ScanEntry>();
            await foreach (var entry in _db.ScanAsync(
                Encoding.UTF8.GetBytes("aaa"),
                Encoding.UTF8.GetBytes("ccc")))
            {
                results.Add(entry);
            }

            Assert.Equal(3, results.Count);
            Assert.Equal("aaa", Encoding.UTF8.GetString(results[0].Key.Span));
            Assert.Equal("bbb", Encoding.UTF8.GetString(results[1].Key.Span));
            Assert.Equal("ccc", Encoding.UTF8.GetString(results[2].Key.Span));
        }

        // #55
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ScanAcrossMemtableAndFlushedSSTable()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 100, 25))
            };

            var db = new TrimDatabase(options);
            await db.LoadAsync();
            try
            {
                // Write words to fill memtable and trigger flush to SSTable
                var words = CommonData.Words;
                foreach (var word in words)
                {
                    var key = Encoding.UTF8.GetBytes(word);
                    var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                    await db.PutAsync(key, value);
                }

                await Task.Delay(500);

                // Write a known key to current memtable (after flush)
                var postFlushKey = Encoding.UTF8.GetBytes("zzz_post_flush");
                var postFlushValue = Encoding.UTF8.GetBytes("post_flush_value");
                await db.PutAsync(postFlushKey, postFlushValue);

                // Full scan should include keys from both SSTable and memtable
                var results = new List<ScanEntry>();
                await foreach (var entry in db.ScanAsync())
                {
                    results.Add(entry);
                }

                Assert.True(results.Count > 1, "Should have results from multiple sources");

                // Verify sorted order
                for (var i = 1; i < results.Count; i++)
                {
                    var prev = results[i - 1].Key.Span;
                    var curr = results[i].Key.Span;
                    Assert.True(prev.SequenceCompareTo(curr) < 0,
                        $"Keys not sorted at index {i}");
                }

                // The post-flush key should be present
                var found = false;
                foreach (var entry in results)
                {
                    if (entry.Key.Span.SequenceEqual(postFlushKey))
                    {
                        found = true;
                        break;
                    }
                }
                Assert.True(found, "Post-flush key should be in scan results");
            }
            finally
            {
                try { await db.DisposeAsync(); } catch { }
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #56
        [Fact]
        [Trait("Category", "Specification")]
        public async Task RangeScanCrossesMemtableAndSSTableBoundary()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 100, 25))
            };

            var db = new TrimDatabase(options);
            await db.LoadAsync();
            try
            {
                // Write a known key that will land in SSTable
                var targetKey = Encoding.UTF8.GetBytes("range_target");
                var targetValue = Encoding.UTF8.GetBytes("target_value");
                await db.PutAsync(targetKey, targetValue);

                // Fill memtable to trigger flush
                var words = CommonData.Words;
                foreach (var word in words)
                {
                    await db.PutAsync(Encoding.UTF8.GetBytes(word), Encoding.UTF8.GetBytes($"V={word}"));
                }
                await Task.Delay(500);

                // Write keys to current memtable that bracket the range
                await db.PutAsync(Encoding.UTF8.GetBytes("range_aaa"), Encoding.UTF8.GetBytes("v_aaa"));
                await db.PutAsync(Encoding.UTF8.GetBytes("range_zzz"), Encoding.UTF8.GetBytes("v_zzz"));

                // Range scan should pick up keys from both memtable and SSTable
                var results = new List<ScanEntry>();
                await foreach (var entry in db.ScanAsync(
                    Encoding.UTF8.GetBytes("range_a"),
                    Encoding.UTF8.GetBytes("range_z")))
                {
                    results.Add(entry);
                }

                // Should include range_aaa (memtable) and range_target (SSTable)
                Assert.True(results.Count >= 2, $"Expected at least 2 results, got {results.Count}");

                var foundTarget = false;
                var foundAaa = false;
                foreach (var entry in results)
                {
                    var keyStr = Encoding.UTF8.GetString(entry.Key.Span);
                    if (keyStr == "range_target") foundTarget = true;
                    if (keyStr == "range_aaa") foundAaa = true;
                }
                Assert.True(foundTarget, "range_target (from SSTable) should be in range scan");
                Assert.True(foundAaa, "range_aaa (from memtable) should be in range scan");
            }
            finally
            {
                try { await db.DisposeAsync(); } catch { }
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #57
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ScanEmptyDatabaseYieldsNothing()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
            var options = new TrimDatabaseOptions
            {
                DatabaseFolder = folder,
                BlockCache = () => new MMapBlockCache(),
                OpenReadOnly = true,
                DisableManifest = true,
            };

            var db = new TrimDatabase(options);
            await db.LoadAsync();
            try
            {
                var results = new List<ScanEntry>();
                await foreach (var entry in db.ScanAsync())
                {
                    results.Add(entry);
                }

                Assert.Empty(results);
            }
            finally
            {
                try { await db.DisposeAsync(); } catch { }
                try { if (Directory.Exists(folder)) Directory.Delete(folder, true); } catch { }
            }
        }

        // #58
        [Fact]
        [Trait("Category", "Specification")]
        public async Task ScanWithStartKeyPastAllKeysYieldsNothing()
        {
            await _db.PutAsync(Encoding.UTF8.GetBytes("aaa"), Encoding.UTF8.GetBytes("v1"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("bbb"), Encoding.UTF8.GetBytes("v2"));
            await _db.PutAsync(Encoding.UTF8.GetBytes("ccc"), Encoding.UTF8.GetBytes("v3"));

            var results = new List<ScanEntry>();
            await foreach (var entry in _db.ScanAsync(Encoding.UTF8.GetBytes("zzz")))
            {
                results.Add(entry);
            }

            Assert.Empty(results);
        }
    }
}
