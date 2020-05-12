using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage.Blocks.MemoryMappedCache
{
    public class MMapBlockCacheMemory : MemoryManager<byte>
    {
        private readonly MMapBlockCache _blockCache;
        private readonly FileIdentifier _fileId;
        private readonly int _blockId;
        private readonly IntPtr _ptr;

        public MMapBlockCacheMemory(MMapBlockCache blockCache, FileIdentifier fileId, int blockId, IntPtr ptr)
        {
            _blockCache = blockCache;
            _fileId = fileId;
            _blockId = blockId;
            _ptr = IntPtr.Add(ptr, blockId * FileConsts.PageSize);
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
