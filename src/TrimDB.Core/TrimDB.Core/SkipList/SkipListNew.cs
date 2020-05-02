using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.SkipList
{
    public class SkipListNew
    {
        private readonly SkipListAllocator _allocator;

        public SkipListNew(SkipListAllocator allocator)
        {
            _allocator = allocator;
        }

        public bool Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            var valueLocation = _allocator.AllocateValue(value);
            if (valueLocation == 0)
            {
                return false;
            }

            var height = _allocator.CurrentHeight;
            var previous = (Span<long>)stackalloc long[_allocator.MaxHeight + 1];
            var next = (Span<long>)stackalloc long[previous.Length];

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

        public SkipListEnumerator GetIterator() => new SkipListEnumerator(-1, _allocator);

        private (long left, long right) FindBounds(ReadOnlySpan<byte> key, long startPoint, byte height)
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

        public void Delete(ReadOnlySpan<byte> key)
        {
            throw new NotImplementedException();
        }

        public SkipListResult TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            var result = Search(key, out var nextNode);
            if (result == SkipListResult.Found)
            {
                value = _allocator.GetValue(nextNode.ValueLocation);
            }
            else
            {
                value = default;
            }
            return result;
        }

        private SkipListResult Search(ReadOnlySpan<byte> key, out SkipListNode node)
        {
            var currentNode = _allocator.HeadNode;
            var nextNodeLocation = 0l;
            var level = (byte)(_allocator.CurrentHeight - 1);
            while (true)
            {
                nextNodeLocation = currentNode.GetTableLocation(level);
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
                        return SkipListResult.NotFound;
                    }
                }
                node = _allocator.GetNode(nextNodeLocation);
                var compare = key.SequenceCompareTo(node.Key);
                if (compare == 0)
                {
                    return SkipListResult.Found;
                }
                else if (compare > 0)
                {
                    currentNode = node;
                    continue;
                }

                if (level > 0)
                {
                    level--;
                    continue;
                }

                return SkipListResult.NotFound;
            }
        }

        public enum SkipListResult
        {
            NotFound,
            Deleted,
            Found
        }

        public class SkipListEnumerator
        {
            private long _location;
            private SkipListAllocator _allocator;

            public SkipListEnumerator(long location, SkipListAllocator allocator)
            {
                _location = location;
                _allocator = allocator;
            }

            public SkipListNode GetNext()
            {
                SkipListNode nextNode;
                if (_location == -1)
                {
                    nextNode = _allocator.HeadNode;
                }
                else if (_location == 0)
                {
                    //Unallocated end of list node
                    return new SkipListNode();
                }
                else
                {
                    nextNode = _allocator.GetNode(_location);
                }
                _location = nextNode.GetTableLocation(0);
                return nextNode;
            }
        }
    }
}
