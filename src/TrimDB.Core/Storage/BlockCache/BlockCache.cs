using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.BlockCache
{
    public class BlockCache
    {
        private ConcurrentDictionary<FileIdentifier, BlockCacheFile> _cache = new ConcurrentDictionary<FileIdentifier, BlockCacheFile>();

        public void RegisterFile(string fileName, FileIdentifier id)
        {
            var file = new BlockCacheFile(fileName, id, this);
            _cache.TryAdd(id, file);
        }

        public void RemoveFile(FileIdentifier id)
        {
            if (_cache.TryRemove(id, out var file))
            {
                file.Dispose();
            }
        }

        public ValueTask<IMemoryOwner<byte>> GetBlock(FileIdentifier id, int blockId)
        {
            var file = _cache[id];
            var memory = file.GetBlockAsync(blockId);
            return memory;
        }

        internal void ReturnBlock(BlockCacheMemory blockCacheMemory)
        {
            _cache[blockCacheMemory.FileId].MemoryReturned();
        }


        public void RemapFile(FileIdentifier oldFile, FileIdentifier newFile)
        {
            throw new NotImplementedException();
        }
    }

    public class BlockCacheMemory : MemoryManager<byte>
    {
        private BlockCache _blockCache;
        private FileIdentifier _fileId;
        private int _blockId;
        private IntPtr _ptr;

        public BlockCacheMemory(BlockCache blockCache, FileIdentifier fileId, int blockId, IntPtr ptr)
        {
            _blockCache = blockCache;
            _fileId = fileId;
            _blockId = blockId;
            _ptr = ptr;
        }

        public int BlockId => _blockId;
        public FileIdentifier FileId => _fileId;

        public unsafe override Span<byte> GetSpan() => new Span<byte>(_ptr.ToPointer(), FileConsts.PageSize);

        public override MemoryHandle Pin(int elementIndex = 0) => new MemoryHandle();

        public override void Unpin() { }

        protected override void Dispose(bool disposing)
        {
            _blockCache.ReturnBlock(this);
        }
    }
}
