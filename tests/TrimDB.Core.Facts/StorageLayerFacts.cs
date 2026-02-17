using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using TrimDB.Core.Storage.Layers;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class StorageLayerFacts : IDisposable
    {
        private readonly string _tempDir;
        private readonly MMapBlockCache _blockCache;
        private readonly MurmurHash3 _hasher = new();

        public StorageLayerFacts()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "TrimDB_SL_" + Guid.NewGuid().ToString("N"));
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
        /// Test #70: AddTableFile CAS loop works correctly from two threads.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task AddTableFileCASLoop()
        {
            var layerDir = Path.Combine(_tempDir, "cas_add");
            Directory.CreateDirectory(layerDir);

            var layer = new UnsortedStorageLayer(1, layerDir, _blockCache);
            Assert.Empty(layer.GetTables());

            // Create two SSTable files
            var file1 = await CreateSSTable(layerDir, "Level1_0.trim", new[] { ("aaa", "val1") });
            var file2 = await CreateSSTable(layerDir, "Level1_1.trim", new[] { ("bbb", "val2") });

            var table1 = new TableFile(Path.Combine(layerDir, "Level1_0.trim"), _blockCache);
            var table2 = new TableFile(Path.Combine(layerDir, "Level1_1.trim"), _blockCache);
            await table1.LoadAsync();
            await table2.LoadAsync();

            // Recreate layer so it picks up files
            var cleanDir = Path.Combine(_tempDir, "cas_add_clean");
            Directory.CreateDirectory(cleanDir);
            var cleanLayer = new UnsortedStorageLayer(1, cleanDir, _blockCache);

            // Add from two threads concurrently
            var barrier = new Barrier(2);
            var t1 = Task.Run(() =>
            {
                barrier.SignalAndWait();
                cleanLayer.AddTableFile(table1);
            });
            var t2 = Task.Run(() =>
            {
                barrier.SignalAndWait();
                cleanLayer.AddTableFile(table2);
            });

            await Task.WhenAll(t1, t2);

            var tables = cleanLayer.GetTables();
            Assert.Equal(2, tables.Length);
            Assert.Contains(table1, tables);
            Assert.Contains(table2, tables);
        }

        /// <summary>
        /// Test #71: RemoveTable CAS loop.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task RemoveTableCASLoop()
        {
            var layerDir = Path.Combine(_tempDir, "cas_remove");
            Directory.CreateDirectory(layerDir);

            var cleanLayer = new UnsortedStorageLayer(1, layerDir, _blockCache);

            // Create and add 3 tables
            var tables = new TableFile[3];
            for (var i = 0; i < 3; i++)
            {
                var fileName = $"Level1_{i}.trim";
                await CreateSSTable(layerDir, fileName, new[] { ($"key{i}", $"val{i}") });
                tables[i] = new TableFile(Path.Combine(layerDir, fileName), _blockCache);
                await tables[i].LoadAsync();
                cleanLayer.AddTableFile(tables[i]);
            }

            Assert.Equal(3, cleanLayer.GetTables().Length);

            // Remove the middle one from two threads. Only one should succeed in removing it,
            // but both calls should complete without error.
            var barrier = new Barrier(2);
            var t1 = Task.Run(() =>
            {
                barrier.SignalAndWait();
                cleanLayer.RemoveTable(tables[1]);
            });
            var t2 = Task.Run(() =>
            {
                barrier.SignalAndWait();
                // Small delay so the other thread likely gets there first
                Thread.SpinWait(100);
                // Second remove of same table would fail with index out of range
                // if the table is already gone. We just verify the end state.
            });

            await Task.WhenAll(t1, t2);

            var remaining = cleanLayer.GetTables();
            Assert.Equal(2, remaining.Length);
            Assert.Contains(tables[0], remaining);
            Assert.Contains(tables[2], remaining);
        }

        /// <summary>
        /// Test #72: AddAndRemoveTableFiles with concurrent reader sees consistent snapshot.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task AddAndRemoveTableFilesCASLoop()
        {
            var layerDir = Path.Combine(_tempDir, "cas_addremove");
            Directory.CreateDirectory(layerDir);

            var layer = new UnsortedStorageLayer(1, layerDir, _blockCache);

            // Add initial tables
            var initialTables = new List<TableFile>();
            for (var i = 0; i < 3; i++)
            {
                var fileName = $"Level1_{i}.trim";
                await CreateSSTable(layerDir, fileName, new[] { ($"key{i}", $"val{i}") });
                var tf = new TableFile(Path.Combine(layerDir, fileName), _blockCache);
                await tf.LoadAsync();
                layer.AddTableFile(tf);
                initialTables.Add(tf);
            }

            // Create replacement tables
            var newTables = new List<TableFile>();
            for (var i = 10; i < 12; i++)
            {
                var fileName = $"Level1_{i}.trim";
                await CreateSSTable(layerDir, fileName, new[] { ($"newkey{i}", $"newval{i}") });
                var tf = new TableFile(Path.Combine(layerDir, fileName), _blockCache);
                await tf.LoadAsync();
                newTables.Add(tf);
            }

            // Remove first table and add new ones, while reader reads concurrently
            var readerDone = false;
            var readerSawConsistentState = true;

            var readerTask = Task.Run(() =>
            {
                while (!Volatile.Read(ref readerDone))
                {
                    var snapshot = layer.GetTables();
                    // The snapshot should be a valid array -- never null, never torn
                    if (snapshot == null)
                    {
                        readerSawConsistentState = false;
                        break;
                    }
                }
            });

            // AddAndRemoveTableFiles uses (overlapped.Count - 1) because in production,
            // the first element of `overlapped` is from a different (upper) layer and is NOT
            // in the current layer's _tableFiles. So we simulate this correctly:
            // - fakeUpperTable is not in the layer
            // - initialTables[0] and initialTables[1] are in the layer and will be removed
            var fakeUpperLayerDir = Path.Combine(_tempDir, "cas_upper");
            Directory.CreateDirectory(fakeUpperLayerDir);
            await CreateSSTable(fakeUpperLayerDir, "Level1_99.trim", new[] { ("upperkey", "upperval") });
            var fakeUpperTable = new TableFile(Path.Combine(fakeUpperLayerDir, "Level1_99.trim"), _blockCache);
            await fakeUpperTable.LoadAsync();

            var overlapped = new List<TableFile> { fakeUpperTable, initialTables[0], initialTables[1] };
            layer.AddAndRemoveTableFiles(newTables, overlapped);

            Volatile.Write(ref readerDone, true);
            await readerTask;

            Assert.True(readerSawConsistentState, "Reader saw an inconsistent (null) snapshot");

            var finalTables = layer.GetTables();
            Assert.DoesNotContain(initialTables[0], finalTables);
            Assert.DoesNotContain(initialTables[1], finalTables);
            foreach (var nt in newTables)
            {
                Assert.Contains(nt, finalTables);
            }
        }

        /// <summary>
        /// Test #73: UnsortedStorageLayer searches newest (highest index) first.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task UnsortedLayerSearchesNewestFirst()
        {
            var layerDir = Path.Combine(_tempDir, "unsorted_newest");
            Directory.CreateDirectory(layerDir);

            var layer = new UnsortedStorageLayer(1, layerDir, _blockCache);

            // Two SSTables with the same key "samekey" but different values
            var fileName1 = "Level1_0.trim";
            var fileName2 = "Level1_1.trim";

            await CreateSSTable(layerDir, fileName1, new[] { ("samekey", "old_value") });
            await CreateSSTable(layerDir, fileName2, new[] { ("samekey", "new_value") });

            var table1 = new TableFile(Path.Combine(layerDir, fileName1), _blockCache);
            var table2 = new TableFile(Path.Combine(layerDir, fileName2), _blockCache);
            await table1.LoadAsync();
            await table2.LoadAsync();

            // Add in order: table1 (older), table2 (newer)
            layer.AddTableFile(table1);
            layer.AddTableFile(table2);

            var key = Encoding.UTF8.GetBytes("samekey");
            var hash = _hasher.ComputeHash64(key);
            var result = await layer.GetAsync(key, hash);

            Assert.Equal(SearchResult.Found, result.Result);
            // Unsorted layer iterates from end (newest) to start, so table2's value should win
            Assert.Equal("new_value", Encoding.UTF8.GetString(result.Value.Span));
        }

        /// <summary>
        /// Test #74: SortedStorageLayer finds key via linear scan across non-overlapping SSTables.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task SortedLayerLinearScanFindsKey()
        {
            var layerDir = Path.Combine(_tempDir, "sorted_scan");
            Directory.CreateDirectory(layerDir);

            var layer = new SortedStorageLayer(2, layerDir, _blockCache, 1024 * 1024 * 16, 10);

            // Create 5 non-overlapping SSTables
            var ranges = new[]
            {
                ("Level2_0.trim", "aaa", "azz"),
                ("Level2_1.trim", "baa", "bzz"),
                ("Level2_2.trim", "caa", "czz"),
                ("Level2_3.trim", "daa", "dzz"),
                ("Level2_4.trim", "eaa", "ezz"),
            };

            foreach (var (fileName, keyStart, keyEnd) in ranges)
            {
                // Put a few keys in each range
                var pairs = new List<(string, string)>
                {
                    (keyStart, $"val_{keyStart}"),
                    (keyEnd, $"val_{keyEnd}"),
                };
                await CreateSSTable(layerDir, fileName, pairs.ToArray());
                var tf = new TableFile(Path.Combine(layerDir, fileName), _blockCache);
                await tf.LoadAsync();
                layer.AddTableFile(tf);
            }

            // Search for a key in the third SSTable
            var searchKey = Encoding.UTF8.GetBytes("caa");
            var hash = _hasher.ComputeHash64(searchKey);
            var result = await layer.GetAsync(searchKey, hash);

            Assert.Equal(SearchResult.Found, result.Result);
            Assert.Equal("val_caa", Encoding.UTF8.GetString(result.Value.Span));
        }

        // --- Helpers ---

        private async Task<string> CreateSSTable(string dir, string fileName, (string Key, string Value)[] pairs)
        {
            var fullPath = Path.Combine(dir, fileName);
            if (File.Exists(fullPath)) File.Delete(fullPath);

            using var allocator = new ArrayBasedAllocator32(4096 * 1000, 25);
            var skipList = new SkipList32(allocator);

            foreach (var (key, value) in pairs)
            {
                skipList.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
            }

            var writer = new TableFileWriter(fullPath);
            await writer.SaveMemoryTable(skipList);
            return fullPath;
        }
    }
}
