using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;

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
        private Func<StorageLayer, bool> _sortedStrategy;
        private Func<StorageLayer, bool> _unsortedStrategy;

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
            await _writerTask;
            await _mergeTask;
        }

        public ValueTask ScheduleSave(MemoryTable memoryTable) => _channel.Writer.WriteAsync(memoryTable);

        private async Task CheckForMerge()
        {
            while (!_token.IsCancellationRequested)
            {
                foreach (var sl in _database.StorageLayers)
                {
                    switch (sl)
                    {
                        case SortedStorageLayer sortedLayer:
                            if (_sortedStrategy(sortedLayer))
                            {
                                throw new NotImplementedException("We should merge here");
                            }
                            break;
                        case UnsortedStorageLayer unsorted:
                            if (_unsortedStrategy(unsorted))
                            {
                                await MergeUnsortedLayer(unsorted);
                            }
                            break;
                        default:
                            throw new InvalidOperationException("We have a type of storage layer we don't know what to do with");
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100), _token.Token);
            }
        }

        private async Task MergeUnsortedLayer(UnsortedStorageLayer unsortedLayer)
        {
            // We can't merge downwards at the bottom layer
            if (unsortedLayer.Level == _database.StorageLayers.Count) return;

            var nextLayer = _database.StorageLayers[unsortedLayer.Level];

            var tables = unsortedLayer.GetTables();
            var oldestTable = tables[0];

            // We maybe able to push a file straight down look for exclusive ranges
            if (nextLayer.NumberOfTables < nextLayer.MaxFilesAtLayer)
            {
                if (DoesTableFitWithNoOverlap(oldestTable, nextLayer))
                {
                    // Candidate for merging
                    var newFilename = nextLayer.GetNextFileName();
                    var oldFileName = oldestTable.FileName;
                    await oldestTable.LoadToMemory();
                    System.IO.File.Move(oldFileName, newFilename);
                    var newTable = new TableFile(newFilename);
                    await newTable.LoadAsync();
                    nextLayer.AddTableFile(newTable);
                    unsortedLayer.RemoveTable(oldestTable);
                    oldestTable.Dispose();
                    return;
                }
            }

            // Get all of the overlapping tables
            var overlapped = GetOverlappingTables(oldestTable, nextLayer);
            overlapped.Add(oldestTable);

            var merger = new TableFileMerger(overlapped.Select(ol => ol.GetAsyncEnumerator()).ToArray());
            // Begin writing out to disk


            //throw new NotImplementedException();
        }

        private List<TableFile> GetOverlappingTables(TableFile table, StorageLayer nextLayer)
        {
            var tablesBelow = nextLayer.GetTables();
            var firstKey = table.FirstKey.Span;
            var lastKey = table.FirstKey.Span;

            var overlapped = new List<TableFile>();

            foreach(var lowerTable in tablesBelow)
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
                    var tableFile = new TableFile(fileWriter.FileName);
                    await tableFile.LoadAsync();
                    _storageLayer.AddTableFile(tableFile);
                    _database.RemoveMemoryTable(sl);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error " + ex);
                }
                //TODO : Remove the memory table from the database

            }
        }
    }
}
