using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TrimDB.Core.InMemory.SkipList64
{
    public class SkipListEnumerator64 : IEnumerator<IMemoryItem>
    {
        private readonly SkipListAllocator64 _allocator;
        private readonly SkipListItem64 _item;
        private long _currentLocation;

        internal SkipListEnumerator64(SkipListAllocator64 allocator)
        {
            _allocator = allocator;
            _currentLocation = _allocator.HeadNode.Location;
        }

        public IMemoryItem Current => new SkipListItem64(_allocator, _currentLocation);

        object? IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            var nextNode = _allocator.GetNode(_currentLocation).GetTableLocation(0);
            if (nextNode == 0)
            {
                return false;
            }
            _currentLocation = nextNode;
            return true;
        }

        public void Reset()
        {
            _currentLocation = _allocator.HeadNode.Location;
        }
    }

    public struct SkipListItem64 : IMemoryItem
    {
        private readonly SkipListAllocator64 _allocator;
        private readonly long _nodeLocation;

        internal SkipListItem64(SkipListAllocator64 allocator, long nodeLocation)
        {
            _allocator = allocator;
            _nodeLocation = nodeLocation;
        }

        public ReadOnlySpan<byte> Value
        {
            get
            {
                var node = _allocator.GetNode(_nodeLocation);
                if (node.IsDeleted)
                {
                    return default;
                }

                return _allocator.GetValue(node.ValueLocation);
            }
        }

        public ReadOnlySpan<byte> Key => _allocator.GetNode(_nodeLocation).Key;
        public bool IsDeleted => _allocator.GetNode(_nodeLocation).IsDeleted;
    }
}
