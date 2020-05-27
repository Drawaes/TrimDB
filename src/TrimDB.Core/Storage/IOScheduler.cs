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
            _sortedStrategy = (sl) => false;
            _unsortedStrategy = (sl) => sl.NumberOfTables > (sl.MaxFilesAtLayer * 0.8);
            _channel = Channel.CreateBounded<MemoryTable>(new BoundedChannelOptions(maxSkiplistBacklog));
            _writerTask = WriteInMemoryTable();
            _mergeTask = CheckForMerge();
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
                    bool mergeHappened = false;
                    foreach (var sl in _database.StorageLayers)
                    {
                        switch (sl)
                        {
                            case SortedStorageLayer sortedLayer:
                                if (_sortedStrategy(sortedLayer))
                                {
                                    mergeHappened = true;
                                    throw new NotImplementedException("We should merge here");
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
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occured {ex}");
            }
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
                var writer = new TableFileMergeWriter(_database, nextLayer, _database.BlockCache, unsortedLayer.Level);
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
