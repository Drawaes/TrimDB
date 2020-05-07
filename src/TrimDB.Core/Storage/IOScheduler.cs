using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
                                //throw new NotImplementedException("We should merge here");
                            }
                            break;
                        default:
                            throw new InvalidOperationException("We have a type of storage layer we don't know what to do with");
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100), _token.Token);
            }
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
