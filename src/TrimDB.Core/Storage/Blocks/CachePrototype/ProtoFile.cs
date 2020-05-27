using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;
using TrimDB.Core.Interop.Windows;
using static TrimDB.Core.Interop.Windows.Files;

namespace TrimDB.Core.Storage.Blocks.CachePrototype
{
    class ProtoFile
    {
        private SafeFileHandle _fileHandle;
        private string _fileName;
        private CompletionPorts.CompletionPortSafeHandle _portHandle;
        private ConcurrentQueue<IntPtr> _overlappedStructs;

        public ProtoFile(string fileName, CompletionPorts.CompletionPortSafeHandle completionPort, ConcurrentQueue<IntPtr> overlappedStructs)
        {
            _overlappedStructs = overlappedStructs;
            _fileName = fileName;

            _fileHandle = CreateFileW(fileName, FileAccess.GENERIC_READ, System.IO.FileShare.Read,
                IntPtr.Zero, System.IO.FileMode.Open, FileFlag.OVERLAPPED, IntPtr.Zero);
            if (_fileHandle.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new System.IO.FileLoadException($"Unable to load the file {fileName} with error code {error}");
            }

            _portHandle = CompletionPorts.CreateIoCompletionPort(_fileHandle.DangerousGetHandle(), completionPort.DangerousGetHandle(),
               UIntPtr.Zero, (uint)Environment.ProcessorCount);
            if (_portHandle.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new System.IO.FileLoadException($"Unable to map to the completion port for the file {fileName} with error code {error}");
            }
        }

        public unsafe void ReadBlock(IntPtr buffer, BlockIdentifier bid)
        {
            if (!_overlappedStructs.TryDequeue(out var overLappedPointer))
            {
                overLappedPointer = Marshal.AllocHGlobal(Unsafe.SizeOf<OverlappedStruct>());
            }

            ref var overlapped = ref Unsafe.AsRef<OverlappedStruct>((void*)overLappedPointer);
            overlapped.hEvent = IntPtr.Zero;
            overlapped.Internal = IntPtr.Zero;
            overlapped.InternalHigh = IntPtr.Zero;
            overlapped.Offset = bid.BlockId * FileConsts.PageSize;
            overlapped.OffsetHigh = 0;
            overlapped.Pointer = IntPtr.Zero;
            overlapped.FileId = bid.FileId;
            overlapped.BlockId = (int)bid.BlockId;
            overlapped.LevelId = bid.LevelId;

            ReadFile(_fileHandle, (void*)buffer, FileConsts.PageSize, out var readBytes, overLappedPointer);
        }

        internal void Dispose()
        {
            _fileHandle.Dispose();
        }
    }
}
