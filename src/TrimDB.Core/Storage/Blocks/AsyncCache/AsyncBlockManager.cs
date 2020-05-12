using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.Blocks.AsyncCache
{
    internal class AsyncBlockManager
    {
        private TaskCompletionSource<IMemoryOwner<byte>> _taskSource = new TaskCompletionSource<IMemoryOwner<byte>>();
        
        public IMemoryOwner<byte> BlockMemory { get; set; }
        public Task<IMemoryOwner<byte>> Task => _taskSource.Task;

        public void CompleteSuccess()
        {
            _taskSource.SetResult(BlockMemory);
        }
    }
}
