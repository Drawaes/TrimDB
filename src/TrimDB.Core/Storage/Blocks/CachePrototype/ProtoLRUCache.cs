using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO.Pipes;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage.Blocks.CachePrototype
{
    internal class ProtoLRUCache
    {
        private LinkedList<ProtoBlock> _lruList = new LinkedList<ProtoBlock>();
        private Dictionary<BlockIdentifier, LinkedListNode<ProtoBlock>> _lookup = new Dictionary<BlockIdentifier, LinkedListNode<ProtoBlock>>();
        private List<LinkedListNode<ProtoBlock>> _referencedBlocks = new List<LinkedListNode<ProtoBlock>>();
        private int _currentOffset;
        private object _lock = new object();
        private byte[] _pinnedSlab;
        private int _maxBlocks;
        private GCHandle _gcHandle;
        private ConcurrentDictionary<FileIdentifier, ProtoFile> _files;

        public ProtoLRUCache(int maxBlocks, ConcurrentDictionary<FileIdentifier, ProtoFile> files)
        {
            _files = files;
            _maxBlocks = maxBlocks;
            _pinnedSlab = new byte[(maxBlocks + 1) * FileConsts.PageSize];
            _gcHandle = GCHandle.Alloc(_pinnedSlab, GCHandleType.Pinned);

            _currentOffset = AlignLength(_gcHandle.AddrOfPinnedObject().ToInt64());
        }

        public static int AlignLength(long memoryAddress) => (int)(((memoryAddress + (FileConsts.PageSize - 1)) & ~(FileConsts.PageSize - 1)) - memoryAddress);


        public async Task<IMemoryOwner<byte>> GetMemory(FileIdentifier fileId, int blockId)
        {
            var bid = new BlockIdentifier() { BlockId = (uint)blockId, FileId = (ushort)fileId.FileId, LevelId = (ushort)fileId.Level };
            LinkedListNode<ProtoBlock> node;
            ProtoBlock block = null;

            lock (_lock)
            {
                if (_lookup.TryGetValue(bid, out node))
                {
                    if (node.Value.RefCount == 0)
                    {
                        _lruList.Remove(node);
                        _referencedBlocks.Add(node);
                    }
                    node.Value.RefCount++;
                    ProtoEventSource.Log.ReportCacheHit();
                }
                else
                {
                    ProtoEventSource.Log.ReportCacheMiss();
                    if (_lookup.Count < _maxBlocks)
                    {
                        var offset = _currentOffset;
                        _currentOffset += FileConsts.PageSize;

                        block = new ProtoBlock(bid, offset);
                        node = new LinkedListNode<ProtoBlock>(block);
                        _lookup.Add(bid, node);
                        _referencedBlocks.Add(node);
                    }
                    else
                    {
                        var lastNode = _lruList.Last;
                        if (lastNode == null)
                        {
                            throw new NotImplementedException("We have no space at the inn, we need to figure out what we do here so we don't deadlock");
                        }
                        else
                        {
                            _lookup.Remove(lastNode.Value.BlockId);
                            _lruList.RemoveLast();
                            block = new ProtoBlock(bid, lastNode.Value.Offset);
                            node = new LinkedListNode<ProtoBlock>(block);
                            _lookup.Add(bid, node);
                            _referencedBlocks.Add(node);
                        }
                    }
                }
            }

            if (block != null)
            {
                _files[fileId].ReadBlock(IntPtr.Add(_gcHandle.AddrOfPinnedObject(), block.Offset), bid);
            }

            await node.Value.Task;
            return new ProtoOwner(_pinnedSlab, node.Value.Offset, this, bid);
        }

        internal void CompleteRead(FileIdentifier id, int blockId)
        {
            LinkedListNode<ProtoBlock> node;
            lock(_lock)
            {
                node = _lookup[new BlockIdentifier() { BlockId = (uint)blockId, FileId = (ushort)id.FileId, LevelId = (ushort)id.Level }];
            }
            node.Value.Complete();
        }

        internal void ReturnReference(BlockIdentifier bid)
        {
            lock (_lock)
            {
                if (!_lookup.TryGetValue(bid, out var node))
                {
                    throw new NotImplementedException();
                }
                node.Value.RefCount--;
                if (node.Value.RefCount == 0)
                {
                    _referencedBlocks.Remove(node);
                    _lruList.AddFirst(node);
                }
            }
        }
    }
}
