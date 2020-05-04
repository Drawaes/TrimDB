using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TrimDB.Core.SkipList
{
    public class SkipListEnumerator : IEnumerator<SkipListItem>
    {
        private SkipListAllocator _allocator;
        private SkipListItem _item;
        private long _currentLocation;

        internal SkipListEnumerator(SkipListAllocator allocator)
        {
            _allocator = allocator;
            _currentLocation = _allocator.HeadNode.Location;
        }

        public SkipListItem Current => new SkipListItem(_allocator, _currentLocation);

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

    public struct SkipListItem
    {
        private SkipListAllocator _allocator;
        private long _nodeLocation;

        internal SkipListItem(SkipListAllocator allocator, long nodeLocation)
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
