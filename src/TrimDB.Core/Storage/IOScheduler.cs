using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrimDB.Core.SkipList;

namespace TrimDB.Core.Storage
{
    public class IOScheduler : IAsyncDisposable
    {
        private Channel<SkipList.SkipList> _channel;
        private CancellationTokenSource _token = new CancellationTokenSource();
        private Task _writerTask;
        private UnsortedStorageLayer _storageLayer;

        public IOScheduler(int maxSkiplistBacklog, UnsortedStorageLayer storageLayer)
        {
            _storageLayer = storageLayer;
            _channel = Channel.CreateBounded<SkipList.SkipList>(new BoundedChannelOptions(maxSkiplistBacklog));
            _writerTask = WriteSkipLists();
        }

        public async ValueTask DisposeAsync()
        {
            _channel.Writer.Complete();
            _token.Cancel();
            await _writerTask;
        }

        public ValueTask ScheduleSkipListSave(SkipList.SkipList skipList) => _channel.Writer.WriteAsync(skipList);

        private async Task WriteSkipLists()
        {
            await foreach (var sl in _channel.Reader.ReadAllAsync())
            {
                try
                {


                    var nextFilename = _storageLayer.GetNextFileName();
                    var fileWriter = new TableFileWriter(nextFilename);
                    await fileWriter.SaveSkipList(sl);
                    var tableFile = new TableFile(fileWriter.FileName);
                    await tableFile.LoadAsync();
                    _storageLayer.AddTableFile(new TableFile(fileWriter.FileName));
                }
                catch(Exception ex)
                {
                    Console.WriteLine("Error " + ex);
                }
                //TODO : Remove the skiplist from the database

            }
        }
    }
}
