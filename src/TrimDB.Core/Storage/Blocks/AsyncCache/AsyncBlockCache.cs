using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Interop.Windows;
using static TrimDB.Core.Interop.Windows.CompletionPorts;

namespace TrimDB.Core.Storage.Blocks.AsyncCache
{
    public class AsyncBlockCache : BlockCache
    {
        private ConcurrentDictionary<FileIdentifier, AsyncBlockCacheFile> _cache = new ConcurrentDictionary<FileIdentifier, AsyncBlockCacheFile>();
        private AsyncBlockAllocator _allocator = new AsyncBlockAllocator(4000, 4096);
        private CompletionPortSafeHandle _completionPort;
        private System.Threading.Thread[] _threads = new System.Threading.Thread[Environment.ProcessorCount];

        public AsyncBlockCache()
        {
            _completionPort = CreateIoCompletionPort((IntPtr)(-1), IntPtr.Zero, UIntPtr.Zero, (uint)_threads.Length);
            if (_completionPort.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new System.IO.FileLoadException($"Unable to create a completion port with code {error}");
            }

            for (var i = 0; i < _threads.Length; i++)
            {
                var thread = new System.Threading.Thread(IOThreadLoop);
                thread.IsBackground = true;
                thread.Start();
                _threads[i] = thread;
            }
        }

        internal AsyncBlockAllocator Allocator => _allocator;
        internal CompletionPortSafeHandle CompletionPort => _completionPort;

        private unsafe void IOThreadLoop()
        {
            while (true)
            {
                if (!GetQueuedCompletionStatus(_completionPort, out var numBytesTransfered, out _, out var overlappedPtr, -1))
                {
                    var error = Marshal.GetLastWin32Error();
                    if (error == (int)Errors.NTError.ERROR_ABANDONED_WAIT_0)
                    {
                        return;
                    }
                    throw new NotImplementedException($"There was either an error with the completion port or an IO error to handle error code {error}");
                }
                var overlapped = Unsafe.AsRef<Files.OverlappedStruct>((void*)overlappedPtr);

                var id = new FileIdentifier(overlapped.LevelId, overlapped.FileId);
                if (!_cache.TryGetValue(id, out var blockFile))
                {
                    throw new NotImplementedException("The block file seems to have gone missing since queing a read?");
                }
                blockFile.CompleteBlock(overlapped.BlockId);
            }
        }

        public override ValueTask<IMemoryOwner<byte>> GetBlock(FileIdentifier id, int blockId)
        {
            if (_cache.TryGetValue(id, out var blockFile))
            {
                return blockFile.GetBlockAsync(blockId);
            }

            throw new NotImplementedException();
        }

        public override void RegisterFile(string fileName, int blockCount, FileIdentifier id)
        {
            var file = new AsyncBlockCacheFile(fileName, blockCount, id, this);
            if (!_cache.TryAdd(id, file))
            {
                throw new NotImplementedException("We couldnt' add the file which seems like something has gone wrong");
            }
        }

        public override void RemoveFile(FileIdentifier id)
        {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            foreach (var file in _cache.Values)
            {
                file.Dispose();
            }
            _completionPort.Dispose();
        }
    }
}
