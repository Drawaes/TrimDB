using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Interop.Windows;
using static TrimDB.Core.Interop.Windows.CompletionPorts;

namespace TrimDB.Core.Storage.Blocks.CachePrototype
{
    public class ProtoBlockCache : BlockCache
    {
        private ConcurrentDictionary<FileIdentifier, ProtoFile> _cache = new ConcurrentDictionary<FileIdentifier, ProtoFile>();
        private ProtoLRUCache _lruCache;
        private CompletionPortSafeHandle _completionPort;
        private System.Threading.Thread[] _threads = new System.Threading.Thread[Environment.ProcessorCount];
        private ConcurrentQueue<IntPtr> _overlappedStructs = new ConcurrentQueue<IntPtr>();

        public ProtoBlockCache(int maxBlocks)
        {
            _lruCache = new ProtoLRUCache(maxBlocks, _cache);

            _completionPort = CreateIoCompletionPort((IntPtr)(-1), IntPtr.Zero, UIntPtr.Zero, (uint)_threads.Length);
            if (_completionPort.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new FileLoadException($"Unable to create a completion port with code {error}");
            }

            for (var i = 0; i < _threads.Length; i++)
            {
                var thread = new System.Threading.Thread(IOThreadLoop);
                thread.IsBackground = true;
                thread.Start();
                _threads[i] = thread;
            }
        }

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
                _lruCache.CompleteRead(id, overlapped.BlockId);
                _overlappedStructs.Enqueue((IntPtr)overlappedPtr);
            }
        }

        public override async ValueTask<IMemoryOwner<byte>> GetBlock(FileIdentifier id, int blockId) => await _lruCache.GetMemory(id, blockId);

        public override void RegisterFile(string fileName, int blockCount, FileIdentifier id)
        {
            if (!_cache.TryAdd(id, new ProtoFile(fileName, _completionPort, _overlappedStructs)))
            {
                throw new NotImplementedException();
            }
        }

        public override void RemoveFile(FileIdentifier id)
        {
            throw new NotImplementedException();
        }
    }
}
