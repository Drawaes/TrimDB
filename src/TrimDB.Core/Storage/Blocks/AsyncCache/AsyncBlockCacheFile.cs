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
    public class AsyncBlockCacheFile
    {
        private string _fileName;
        private FileIdentifier _id;
        private AsyncBlockManager[] _blocks;
        private SafeFileHandle _fileHandle;
        private AsyncBlockCache _cache;
        private CompletionPorts.CompletionPortSafeHandle _completionPort;

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

            _completionPort = CompletionPorts.CreateIoCompletionPort(_fileHandle.DangerousGetHandle(), _cache.CompletionPort.DangerousGetHandle(),
               UIntPtr.Zero, (uint)Environment.ProcessorCount);
            if (_completionPort.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new System.IO.FileLoadException($"Unable to map to the completion port for the file {fileName} with error code {error}");
            }
        }

        internal unsafe ValueTask<IMemoryOwner<byte>> GetBlockAsync(int blockId)
        {
            while (true)
            {
                var currentBlock = _blocks[blockId];
                if (currentBlock != null) return new ValueTask<IMemoryOwner<byte>>(currentBlock.Task);

                // TODO: COMPARE EXCHANGE
                var newBlock = new AsyncBlockManager();
                _blocks[blockId] = newBlock;

                newBlock.BlockMemory = _cache.Allocator.Rent(0);

                var overLappedPointer = Marshal.AllocHGlobal(Unsafe.SizeOf<OverlappedStruct>());
                ref var overlapped = ref Unsafe.AsRef<OverlappedStruct>((void*)overLappedPointer);
                overlapped.hEvent = IntPtr.Zero;
                overlapped.Internal = IntPtr.Zero;
                overlapped.InternalHigh = IntPtr.Zero;
                overlapped.Offset = (uint)blockId * FileConsts.PageSize;
                overlapped.OffsetHigh = 0;
                overlapped.Pointer = IntPtr.Zero;
                overlapped.FileId = _id.FileId;
                overlapped.BlockId = blockId;
                overlapped.LevelId = _id.Level;

                ReadFile(_fileHandle, newBlock.BlockMemory.Memory.Pin().Pointer, FileConsts.PageSize, out var readBytes, overLappedPointer);

                return new ValueTask<IMemoryOwner<byte>>(newBlock.Task);
            }
        }

        internal void CompleteBlock(int blockId) => _blocks[blockId].CompleteSuccess();
    }
}
