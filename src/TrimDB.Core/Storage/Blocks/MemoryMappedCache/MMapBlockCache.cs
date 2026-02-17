using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.Blocks.MemoryMappedCache
{
    public class MMapBlockCache : BlockCache
    {
        private readonly ConcurrentDictionary<FileIdentifier, MMapBlockCacheFile> _cache = new ConcurrentDictionary<FileIdentifier, MMapBlockCacheFile>();

        public override ValueTask<IMemoryOwner<byte>> GetBlock(FileIdentifier id, int blockId)
        {
            var file = _cache[id];
            var memory = file.GetBlockAsync(blockId);
            return memory;
        }

        internal void ReturnBlock(MMapBlockCacheMemory blockCacheMemory)
        {
            _cache[blockCacheMemory.FileId].MemoryReturned();
        }

        public override void RegisterFile(string fileName, int blockCount, FileIdentifier id)
        {
            var file = new MMapBlockCacheFile(fileName, blockCount, id, this);
            _cache.TryAdd(id, file);
        }

        public override void RemoveFile(FileIdentifier id)
        {
            if (_cache.TryRemove(id, out var file))
            {
                file.Dispose();
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing) return;
            foreach (var bcf in _cache.Values)
            {
                bcf.Dispose();
            }
            _cache.Clear();
        }

    }
}
