using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.Blocks.MemoryMappedCache
{
    public class MMapBlockCacheFile : IDisposable
    {
        private readonly string _fileName;
        private readonly MemoryMappedFile _mappedFile;
        private readonly MemoryMappedViewAccessor _mappedView;
        private readonly IntPtr _ptr;
        private readonly FileIdentifier _fileId;
        private readonly MMapBlockCache _parentCache;
        private int _numberOfRefs;
        private readonly int _blockCount;

        public unsafe MMapBlockCacheFile(string fileName, int blockCount, FileIdentifier fileId, MMapBlockCache parentCache)
        {
            _blockCount = blockCount;
            _fileId = fileId;
            _parentCache = parentCache;
            _fileName = fileName;
            _mappedFile = MemoryMappedFile.CreateFromFile(_fileName);

            _mappedView = _mappedFile.CreateViewAccessor();
            byte* ptr = null;
            _mappedView.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            _ptr = (IntPtr)ptr;
        }

        internal ValueTask<IMemoryOwner<byte>> GetBlockAsync(int blockId)
        {
            if (blockId >= _blockCount) throw new ArgumentOutOfRangeException($"You requested a block of {blockId} but the block count is only {_blockCount}");

            Interlocked.Increment(ref _numberOfRefs);
            var mem = new MMapBlockCacheMemory(_parentCache, _fileId, blockId, _ptr);
            return new ValueTask<IMemoryOwner<byte>>(mem);
        }

        internal void MemoryReturned()
        {
            Interlocked.Decrement(ref _numberOfRefs);
        }

        public void Dispose()
        {
            _mappedView.SafeMemoryMappedViewHandle.ReleasePointer();
            _mappedView.Dispose();
            _mappedFile.Dispose();
            GC.SuppressFinalize(this);
        }

        ~MMapBlockCacheFile()
        {
            _mappedView.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}
