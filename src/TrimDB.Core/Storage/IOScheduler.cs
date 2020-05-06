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
        private readonly UnsortedStorageLayer _storageLayer;

        public IOScheduler(int maxSkiplistBacklog, UnsortedStorageLayer storageLayer)
        {
            _storageLayer = storageLayer;
            _channel = Channel.CreateBounded<MemoryTable>(new BoundedChannelOptions(maxSkiplistBacklog));
            _writerTask = WriteInMemoryTable();
        }

        public async ValueTask DisposeAsync()
        {
            _channel.Writer.Complete();
            _token.Cancel();
            await _writerTask;
        }

        public ValueTask ScheduleSave(MemoryTable memoryTable) => _channel.Writer.WriteAsync(memoryTable);

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
                    _storageLayer.AddTableFile(new TableFile(fileWriter.FileName));
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
