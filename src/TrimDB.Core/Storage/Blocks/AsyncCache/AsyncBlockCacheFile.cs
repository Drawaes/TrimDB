using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using TrimDB.Core.Interop.Windows;
using static TrimDB.Core.Interop.Windows.Files;

namespace TrimDB.Core.Storage.Blocks.AsyncCache
{
    public class AsyncBlockCacheFile : IDisposable
    {
        private string _fileName;
        private readonly FileIdentifier _id;
        private readonly AsyncBlockManager?[] _blocks;
        private readonly SafeFileHandle _fileHandle;
        private readonly AsyncBlockCache _cache;
        private readonly CompletionPorts.CompletionPortSafeHandle _cpHandle;

        public AsyncBlockCacheFile(string fileName, int blockCount, FileIdentifier id, AsyncBlockCache cache)
        {
            _cache = cache;
            _blocks = new AsyncBlockManager[blockCount];
            _fileName = fileName;
            _id = id;

            _fileHandle = CreateFileW(fileName, FileAccess.GENERIC_READ, System.IO.FileShare.Read,
                IntPtr.Zero, System.IO.FileMode.Open, FileFlag.OVERLAPPED, IntPtr.Zero);
            if (_fileHandle.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new System.IO.FileLoadException($"Unable to load the file {fileName} with error code {error}");
            }

            _cpHandle = CompletionPorts.CreateIoCompletionPort(_fileHandle.DangerousGetHandle(), _cache.CompletionPort.DangerousGetHandle(),
               UIntPtr.Zero, (uint)Environment.ProcessorCount);
            if (_cpHandle.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new System.IO.FileLoadException($"Unable to map to the completion port for the file {fileName} with error code {error}");
            }
        }

        internal void RemoveBlock(int blockId) => _blocks[blockId] = null;

        internal ValueTask<IMemoryOwner<byte>> GetBlockAsync(AsyncBlockManager block)
        {
            if (block.Task.IsCompletedSuccessfully)
            {
                return new ValueTask<IMemoryOwner<byte>>(block.GetMemoryManager());
            }

            return InternalGetBlockAsync(block);

            static async ValueTask<IMemoryOwner<byte>> InternalGetBlockAsync(AsyncBlockManager block)
            {
                await block.Task;
                return block.GetMemoryManager();
            }
        }
        internal AsyncBlockAllocator Allocator => _cache.Allocator;

        internal unsafe ValueTask<IMemoryOwner<byte>> GetBlockAsync(int blockId)
        {
            while (true)
            {
                var currentBlock = _blocks[blockId];
                if (currentBlock != null)
                {
                    return GetBlockAsync(currentBlock);
                }

                var newBlock = new AsyncBlockManager(this);

                if (Interlocked.CompareExchange(ref _blocks[blockId], newBlock, currentBlock) != currentBlock)
                {
                    continue;
                }

                _blocks[blockId] = newBlock;

                newBlock.BlockMemory = _cache.Allocator.Rent(0);

                var overLappedPointer = Marshal.AllocHGlobal(Unsafe.SizeOf<OverlappedStruct>());
                ref var overlapped = ref Unsafe.AsRef<OverlappedStruct>((void*)overLappedPointer);
                overlapped.hEvent = IntPtr.Zero;
                overlapped.Internal = IntPtr.Zero;
                overlapped.InternalHigh = IntPtr.Zero;
                long fileOffset = (long)blockId * FileConsts.PageSize;
                overlapped.Offset = (uint)(fileOffset & 0xFFFF_FFFF);
                overlapped.OffsetHigh = (uint)(fileOffset >> 32);
                overlapped.Pointer = IntPtr.Zero;
                overlapped.FileId = _id.FileId;
                overlapped.BlockId = blockId;
                overlapped.LevelId = _id.Level;

                ReadFile(_fileHandle, newBlock.BlockMemory.Memory.Pin().Pointer, FileConsts.PageSize, out var readBytes, overLappedPointer);

                return GetBlockAsync(newBlock);
            }
        }

        internal void CompleteBlock(int blockId) => _blocks[blockId]?.CompleteSuccess();

        public void Dispose()
        {
            _fileHandle.Dispose();
        }
    }
}
