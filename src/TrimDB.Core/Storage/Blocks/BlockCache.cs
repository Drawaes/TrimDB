using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.Blocks
{
    public abstract class BlockCache : IDisposable
    {
        public abstract void RegisterFile(string fileName, int blockCount, FileIdentifier id);

        public abstract void RemoveFile(FileIdentifier id);

        public abstract ValueTask<IMemoryOwner<byte>> GetBlock(FileIdentifier id, int blockId);

        protected virtual void Dispose(bool disposing) { }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        ~BlockCache()
        {
            Dispose(disposing: false);
        }
    }
}
