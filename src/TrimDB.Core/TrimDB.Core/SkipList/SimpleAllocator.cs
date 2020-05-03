using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TrimDB.Core.SkipList
{
    public sealed class SimpleAllocator : SkipListAllocator
    {
        private long _currentSize;
        private readonly List<byte[]> _buffer = new List<byte[]>();
        private readonly List<byte[]> _valueBuffer = new List<byte[]>();
        private readonly long _headNodeLocation;
        private readonly long _maxSize;

        public SimpleAllocator(long maxSize, byte maxHeight)
        : base(maxHeight)
        {
            _maxSize = maxSize;
            _buffer.Add(null);

            var headNodeSize = SkipListNode.CalculateSizeNeeded(maxHeight, 0);
            _headNodeLocation = AllocateNode(headNodeSize, out var memory);
            _ = new SkipListNode(memory, _headNodeLocation, maxHeight, Array.Empty<byte>());
        }

        public override SkipListNode HeadNode => GetNode(_headNodeLocation);

        public override long AllocateValue(ReadOnlySpan<byte> value)
        {
            var length = value.Length;
            var totalSize = Interlocked.Add(ref _currentSize, length);
            if (totalSize > _maxSize)
            {
                return 0;
            }

            var memory = new byte[length];
            long location;

            lock (_valueBuffer)
            {
                location = _valueBuffer.Count;
                _valueBuffer.Add(memory);
            }

            value.CopyTo(memory);
            return location;
        }

        public override SkipListNode GetNode(long nodeLocation)
        {
            byte[] memory;
            lock (_buffer)
            {
                memory = _buffer[(int)nodeLocation];
            }
            return new SkipListNode(memory, nodeLocation);
        }

        public override ReadOnlySpan<byte> GetValue(long valueLocation)
        {
            lock (_valueBuffer)
            {
                return _valueBuffer[(int)valueLocation];
            }
        }

        protected override void Dispose(bool isDisposing)
        {
            // GC'd memory no need to dispose anything
        }

        protected override long AllocateNode(int length, out Span<byte> memory)
        {
            var totalSize = Interlocked.Add(ref _currentSize, length);
            if (totalSize > _maxSize)
            {
                memory = default;
                return 0;
            }

            var memoryArray = new byte[length];

            long location;

            lock (_buffer)
            {
                location = _buffer.Count;
                _buffer.Add(memoryArray);
            }
            memory = memoryArray;
            return location;
        }

    }
}
