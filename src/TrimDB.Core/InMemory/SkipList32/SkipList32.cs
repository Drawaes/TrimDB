using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using static TrimDB.Core.Interop.Windows.Memory;

namespace TrimDB.Core.InMemory.SkipList32
{
    public class SkipList32 : MemoryTable, IDisposable
    {
        private readonly ArrayBasedAllocator32 _allocator;
        private long _currentWriters;

        public SkipList32(ArrayBasedAllocator32 allocator) => _allocator = allocator;

        private void IncrementWriter()
        {
            Interlocked.Increment(ref _currentWriters);
        }

        private void DecrementWriter()
        {
            Interlocked.Decrement(ref _currentWriters);
        }

        internal void WaitForAbilityToWriteToDisk()
        {
            if (Volatile.Read(ref _currentWriters) == 0)
            {
                return;
            }

            while (true)
            {
                Thread.SpinWait(1000);

                if (Volatile.Read(ref _currentWriters) == 0)
                {
                    return;
                }

                Thread.Sleep(100);

                if (Volatile.Read(ref _currentWriters) == 0)
                {
                    return;
                }
            }
        }

        public override bool Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            IncrementWriter();
            try
            {
                var valueLocation = _allocator.AllocateValue(value);
                if (valueLocation == 0)
                {
                    return false;
                }

                var height = _allocator.CurrentHeight;
                var previous = (Span<int>)stackalloc int[_allocator.MaxHeight + 1];
                var next = (Span<int>)stackalloc int[previous.Length];

                previous[height] = _allocator.HeadNode.Location;
                next[height] = 0;

                for (var i = (height - 1); i >= 0; i--)
                {
                    (previous[i], next[i]) = FindBounds(key, previous[i + 1], (byte)i);
                    if (previous[i] == next[i])
                    {
                        var node = _allocator.GetNode(previous[i]);
                        node.SetValueLocation(valueLocation);
                        return true;
                    }
                }

                // Create a new node
                var newNode = _allocator.AllocateNode(key);
                if (!newNode.IsAllocated)
                {
                    return false;
                }
                newNode.SetValueLocation(valueLocation);

                var tableHeight = newNode.TableHeight;
                for (byte i = 0; i < tableHeight; i++)
                {
                    while (true)
                    {
                        if (previous[i] == 0)
                        {
                            (previous[i], next[i]) = FindBounds(key, _allocator.HeadNode.Location, i);
                        }

                        var nextLocation = next[i];
                        newNode.SetTablePointer(i, nextLocation);
                        var prev = _allocator.GetNode(previous[i]);

                        // Set the previous node to now point to us, only if another node 
                        // hasn't been pointed to since we checked
                        if (prev.SetTablePointer(i, nextLocation, newNode.Location))
                        {
                            break;
                        }

                        // Another thread must have inserted a new node since we searched
                        // We now need to search again however we know that the node must have 
                        // been inserted after previous so we can just check from previous

                        (previous[i], next[i]) = FindBounds(key, previous[i], i);
                        if (previous[i] == next[i])
                        {
                            _allocator.GetNode(previous[i]).SetValueLocation(valueLocation);
                            return true;
                        }
                    }
                }
                return true;
            }
            finally
            {
                DecrementWriter();
            }
        }

        private (int left, int right) FindBounds(ReadOnlySpan<byte> key, int startPoint, byte height)
        {
            var startNode = _allocator.GetNode(startPoint);
            while (true)
            {
                var nextLocation = startNode.GetTableLocation(height);
                if (nextLocation == 0)
                {
                    return (startPoint, nextLocation);
                }
                var nextNode = _allocator.GetNode(nextLocation);
                var compareResult = key.SequenceCompareTo(nextNode.Key);
                if (compareResult < 0)
                {
                    return (startPoint, nextLocation);
                }
                if (compareResult == 0)
                {
                    return (nextLocation, nextLocation);
                }
                startPoint = nextLocation;
                startNode = nextNode;
            }
        }

        public override bool Delete(ReadOnlySpan<byte> key)
        {
            var result = Search(key, out var nextNode);
            if (result == SearchResult.Found)
            {
                nextNode.SetDeleted();
                return true;
            }
            if (result == SearchResult.Deleted)
            {
                return true;
            }
            // Key not in memtable — insert a tombstone so it shadows disk entries
            if (!Put(key, ReadOnlySpan<byte>.Empty))
            {
                return false;
            }
            result = Search(key, out nextNode);
            if (result == SearchResult.Found)
            {
                nextNode.SetDeleted();
            }
            return true;
        }

        public override SearchResult TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            var result = Search(key, out var nextNode);
            if (result == SearchResult.Found)
            {
                value = _allocator.GetValue(nextNode.ValueLocation);
            }
            else
            {
                value = default;
            }
            return result;
        }

        private SearchResult Search(ReadOnlySpan<byte> key, out SkipListNode32 node)
        {
            var currentNode = _allocator.HeadNode;
            var level = _allocator.CurrentHeight;
            while (true)
            {
                var nextNodeLocation = currentNode.GetTableLocation(level);
                if (nextNodeLocation == 0)
                {
                    if (level > 0)
                    {
                        level--;
                        continue;
                    }
                    else
                    {
                        node = default;
                        return SearchResult.NotFound;
                    }
                }
                var nextNode = _allocator.GetNode(nextNodeLocation);
                var compare = key.SequenceCompareTo(nextNode.Key);
                if (compare == 0)
                {
                    node = nextNode;
                    if (node.IsDeleted)
                    {
                        return SearchResult.Deleted;
                    }
                    else
                    {
                        return SearchResult.Found;
                    }
                }
                else if (compare > 0)
                {
                    currentNode = nextNode;
                    continue;
                }

                if (level > 0)
                {
                    level--;
                    continue;
                }

                node = default;
                return SearchResult.NotFound;
            }
        }

        public override IEnumerator<IMemoryItem> GetEnumerator() => new SkipListEnumerator32(_allocator);

        public void Dispose()
        {
            
        }
    }
}
