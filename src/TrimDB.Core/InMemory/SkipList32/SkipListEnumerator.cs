using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.InMemory.SkipList32
{
    public class SkipListEnumerator32 : IEnumerator<IMemoryItem>
    {
        private readonly NativeAllocator32 _allocator;
        private int _currentLocation;

        internal SkipListEnumerator32(NativeAllocator32 allocator)
        {
            _allocator = allocator;
            _currentLocation = _allocator.HeadNode.Location;
        }

        public IMemoryItem Current => new SkipListItem32(_allocator, _currentLocation);

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

    public struct SkipListItem32 : IMemoryItem
    {
        private readonly NativeAllocator32 _allocator;
        private readonly int _nodeLocation;

        internal SkipListItem32(NativeAllocator32 allocator, int nodeLocation)
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
