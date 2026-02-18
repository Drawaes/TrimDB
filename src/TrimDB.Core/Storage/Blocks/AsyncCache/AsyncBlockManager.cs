using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.Interop.Windows;

namespace TrimDB.Core.Storage.Blocks.AsyncCache
{
    internal class AsyncBlockManager
    {
        private TaskCompletionSource<bool> _taskSource = new TaskCompletionSource<bool>();
        private int _refCount;
        private AsyncBlockCacheFile _cacheFile;

        public IMemoryOwner<byte> BlockMemory { get; set; } = null!;
        public Task<bool> Task => _taskSource.Task;

        public AsyncBlockManager(AsyncBlockCacheFile cacheFile) => _cacheFile = cacheFile;

        public AsyncBlockManagerRefCounter GetMemoryManager()
        {
            Interlocked.Increment(ref _refCount);
            return new AsyncBlockManagerRefCounter(this);
        }

        public int IsReferenced => Volatile.Read(ref _refCount);

        public void CompleteSuccess()
        {
            _taskSource.SetResult(true);
        }

        public void DecrementRefCount()
        {
            var newValue = Interlocked.Decrement(ref _refCount);
        }
    }

    internal class AsyncBlockManagerRefCounter : IMemoryOwner<byte>
    {
        private AsyncBlockManager _manager;

        public AsyncBlockManagerRefCounter(AsyncBlockManager manager) => _manager = manager;

        public Memory<byte> Memory => _manager.BlockMemory.Memory;


        public void Dispose()
        {
            _manager.DecrementRefCount();
        }
    }
}
