using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.BlockCache
{
    public class BlockCacheFile:IDisposable
    {
        private string _fileName;
        private MemoryMappedFile _mappedFile;
        private MemoryMappedViewAccessor _mappedView;
        private IntPtr _ptr;
        private FileIdentifier _fileId;
        private BlockCache _parentCache;
        private int _numberOfRefs;

        public unsafe BlockCacheFile(string fileName, FileIdentifier fileId, BlockCache parentCache)
        {
            _fileId = fileId;
            _parentCache = parentCache;
            _fileName = fileName;
            _mappedFile = MemoryMappedFile.CreateFromFile(fileName);

            _mappedView = _mappedFile.CreateViewAccessor();
            byte* ptr = null;
            _mappedView.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            _ptr = (IntPtr)ptr;
        }

        internal ValueTask<IMemoryOwner<byte>> GetBlockAsync(int blockId)
        {
            Interlocked.Increment(ref _numberOfRefs);
            var mem = new BlockCacheMemory(_parentCache, _fileId, blockId, _ptr);
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

        ~BlockCacheFile()
        {
            _mappedView.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}
