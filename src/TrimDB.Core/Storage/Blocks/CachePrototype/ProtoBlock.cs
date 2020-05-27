using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.Blocks.CachePrototype
{
    internal class ProtoBlock
    {
        private TaskCompletionSource<bool> _task = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public ProtoBlock(BlockIdentifier id, int offset)
        {
            BlockId = id;
            Offset = offset;
        }

        public int RefCount { get; set; } = 1;
        public int Offset { get; }
        public Task Task => _task.Task;
        public BlockIdentifier BlockId { get; }

        internal void Complete()
        {
            _task.TrySetResult(true);
        }
    }
}
