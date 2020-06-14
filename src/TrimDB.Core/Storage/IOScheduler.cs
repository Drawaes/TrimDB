using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage.Layers;

namespace TrimDB.Core.Storage
{
    public class IOScheduler : IAsyncDisposable
    {
        private readonly Channel<MemoryTable> _channel;
        private readonly CancellationTokenSource _token = new CancellationTokenSource();
        private readonly Task _writerTask;
        private readonly Task _mergeTask;
        private readonly UnsortedStorageLayer _storageLayer;
        private readonly TrimDatabase _database;
        private readonly Func<StorageLayer, bool> _sortedStrategy;
        private readonly Func<StorageLayer, bool> _unsortedStrategy;

        public IOScheduler(int maxSkiplistBacklog, UnsortedStorageLayer storageLayer, TrimDatabase database)
        {
            _storageLayer = storageLayer;
            _database = database;
            _sortedStrategy = (sl) => sl.NumberOfTables > (sl.MaxFilesAtLayer * 0.8);
            _unsortedStrategy = (sl) => sl.NumberOfTables > (sl.MaxFilesAtLayer * 0.8);
            _channel = Channel.CreateBounded<MemoryTable>(new BoundedChannelOptions(maxSkiplistBacklog));
            _writerTask = WriteInMemoryTable();
            if (database.Options.DisableMerging)
            {
                _mergeTask = Task.CompletedTask;
            }
            else
            {
                _mergeTask = CheckForMerge();
            }
        }

        public async ValueTask DisposeAsync()
        {
            _channel.Writer.Complete();
            _token.Cancel();
            Console.WriteLine("About to wait for the writer task");
            await _writerTask;
            Console.WriteLine("About to wait for the merge task");
            await _mergeTask;
            Console.WriteLine("Completed waiting for the merge task");
        }

        public ValueTask ScheduleSave(MemoryTable memoryTable) => _channel.Writer.WriteAsync(memoryTable);

        private async Task CheckForMerge()
        {
            try
            {
                while (!_token.IsCancellationRequested)
                {
                    while (true)
                    {
                        var mergeHappened = false;
                        for (var i = _database.StorageLayers.Count - 2; i >= 0; i--)
                        {
                            var sl = _database.StorageLayers[i];
                            switch (sl)
                            {
                                case SortedStorageLayer sortedLayer:
                                    if (_sortedStrategy(sortedLayer))
                                    {
                                        mergeHappened = true;
                                        await MergeSortedLayer(sortedLayer);
                                    }
                                    break;

                                case UnsortedStorageLayer unsorted:
                                    if (_unsortedStrategy(unsorted))
                                    {
                                        mergeHappened = true;
                                        await MergeUnsortedLayer(unsorted);
                                    }
                                    break;

                                default:
                                    throw new InvalidOperationException("We have a type of storage layer we don't know what to do with");
                            }
                        }
                        if (!mergeHappened)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(50), _token.Token);
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occured {ex}");
            }
        }

        private async Task MergeSortedLayer(SortedStorageLayer sortedStorage)
        {
            var layerBelow = _database.StorageLayers[sortedStorage.Level];
            if (layerBelow.NumberOfTables == 0)
            {
                var file = sortedStorage.GetTables()[0];
                await MoveTableToLowerLevel(sortedStorage, layerBelow, file);

                return;
            }

            var overlapCounts = new int[sortedStorage.NumberOfTables];
            for (var i = 0; i < sortedStorage.GetTables().Length; i++)
            {
                var t = sortedStorage.GetTables()[i];

                // Check if there is overlap
                var overlapCount = 0;

                foreach (var l3t in layerBelow.GetTables())
                {
                    if (t.LastKey.Span.SequenceCompareTo(l3t.FirstKey.Span) < 0)
                    {
                        continue;
                    }
                    if (t.FirstKey.Span.SequenceCompareTo(l3t.LastKey.Span) > 0)
                    {
                        continue;
                    }
                    overlapCount++;
                }

                if (overlapCount == 0)
                {
                    // Move table down one level
                    await MoveTableToLowerLevel(sortedStorage, layerBelow, t);
                    return;
                }
                overlapCounts[i] = overlapCount;
            }

            var min = overlapCounts.Min();
            var indexOfMin = overlapCounts.Select((value, index) => (value, index)).First(i => i.value == min).index;
            var upperTable = sortedStorage.GetTables()[indexOfMin];
            var overlapping = GetOverlappingTables(upperTable, layerBelow);
            overlapping.Insert(0, upperTable);

            Console.WriteLine($"Merging from {sortedStorage.Level}");
            var sw = Stopwatch.StartNew();

            await using (var merger = new TableFileMerger(overlapping.Select(ol => ol.GetAsyncEnumerator()).ToArray()))
            {
                // Begin writing out to disk
                var writer = new TableFileMergeWriter(layerBelow, _database.BlockCache);
                await writer.WriteFromMerger(merger);

                layerBelow.AddAndRemoveTableFiles(writer.NewTableFiles, overlapping);
                sortedStorage.RemoveTable(upperTable);
            }
            Console.WriteLine($"Merge for level {sortedStorage.Level} took {sw.ElapsedMilliseconds}ms");
            foreach (var file in overlapping)
            {
                file.Dispose();
                System.IO.File.Delete(file.FileName);
            }

            // Found with min overlap so merge it
            return;
        }

        private async Task MoveTableToLowerLevel(SortedStorageLayer sortedStorage, StorageLayer layerBelow, TableFile file)
        {
            var newFilename = layerBelow.GetNextFileName();
            System.IO.File.Copy(file.FileName, newFilename);
            var tableFile = new TableFile(newFilename, _database.BlockCache);
            await tableFile.LoadAsync();
            layerBelow.AddTableFile(tableFile);
            sortedStorage.RemoveTable(file);

            file.Dispose();
            System.IO.File.Delete(file.FileName);

            Console.WriteLine("Moved table down with no merge");
            // Move table down one level
        }

        private async Task MergeUnsortedLayer(UnsortedStorageLayer unsortedLayer)
        {
            // We can't merge downwards at the bottom layer
            if (unsortedLayer.Level == _database.StorageLayers.Count) return;

            var nextLayer = _database.StorageLayers[unsortedLayer.Level];

            var tables = unsortedLayer.GetTables();
            var oldestTable = tables[0];

            // Get all of the overlapping tables
            var overlapped = GetOverlappingTables(oldestTable, nextLayer);
            overlapped.Insert(0, oldestTable);

            Console.WriteLine($"Begin merging with {overlapped.Count} tables");
            var sw = Stopwatch.StartNew();

            await using (var merger = new TableFileMerger(overlapped.Select(ol => ol.GetAsyncEnumerator()).ToArray()))
            {
                // Begin writing out to disk
                var writer = new TableFileMergeWriter(nextLayer, _database.BlockCache);
                await writer.WriteFromMerger(merger);

                nextLayer.AddAndRemoveTableFiles(writer.NewTableFiles, overlapped);
                unsortedLayer.RemoveTable(oldestTable);
            }

            foreach (var file in overlapped)
            {
                file.Dispose();
                System.IO.File.Delete(file.FileName);
            }

            Console.WriteLine($"Finished merging in {sw.ElapsedMilliseconds}ms");
        }

        private List<TableFile> GetOverlappingTables(TableFile table, StorageLayer nextLayer)
        {
            var tablesBelow = nextLayer.GetTables();
            var firstKey = table.FirstKey.Span;
            var lastKey = table.LastKey.Span;

            var overlapped = new List<TableFile>();

            foreach (var lowerTable in tablesBelow)
            {
                var compare = firstKey.SequenceCompareTo(lowerTable.LastKey.Span);
                if (compare > 0) continue;
                compare = lastKey.SequenceCompareTo(lowerTable.FirstKey.Span);
                if (compare < 0) continue;
                overlapped.Add(lowerTable);
            }
            return overlapped;
        }

        private bool DoesTableFitWithNoOverlap(TableFile table, StorageLayer nextLayer)
        {
            var tablesBelow = nextLayer.GetTables();

            var noOverlap = true;
            var firstKey = table.FirstKey.Span;
            var lastKey = table.FirstKey.Span;

            foreach (var lowerTable in tablesBelow)
            {
                var compare = firstKey.SequenceCompareTo(lowerTable.LastKey.Span);
                if (compare > 0) continue;
                compare = lastKey.SequenceCompareTo(lowerTable.FirstKey.Span);
                if (compare < 0) continue;
                noOverlap = false;
            }

            return noOverlap;
        }

        private async Task WriteInMemoryTable()
        {
            await foreach (var sl in _channel.Reader.ReadAllAsync())
            {
                try
                {
                    var nextFilename = _storageLayer.GetNextFileName();
                    var fileWriter = new TableFileWriter(nextFilename);
                    await fileWriter.SaveMemoryTable(sl);
                    var tableFile = new TableFile(fileWriter.FileName, _database.BlockCache);
                    await tableFile.LoadAsync();
                    _storageLayer.AddTableFile(tableFile);
                    _database.RemoveMemoryTable(sl);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error " + ex);
                }
            }
        }
    }
}
