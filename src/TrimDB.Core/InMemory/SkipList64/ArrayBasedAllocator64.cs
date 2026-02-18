using System;
using System.Runtime.CompilerServices;
using System.Threading;

#pragma warning disable CS0618 // Obsolete SkipList64 types reference each other

namespace TrimDB.Core.InMemory.SkipList64
{
    [Obsolete("Use SkipList32 instead. Will be removed in a future release.")]
    public class ArrayBasedAllocator64 : SkipListAllocator64
    {
        private long _maxSize;
        private byte[] _data;
        private long _initalOffset;
        private long _currentPointer;

        public ArrayBasedAllocator64(long maxSize, byte maxHeight)
            : base(maxHeight)
        {
            _maxSize = maxSize;
            _data = GC.AllocateArray<byte>((int)(maxSize + ALIGNMENTSIZE - 1), pinned: true);
            unsafe
            {
                fixed (byte* ptr = _data)
                {
                    var addr = (long)ptr;
                    _initalOffset = AlignLength(addr) - addr;
                }
            }
            if (_initalOffset == 0)
            {
                _initalOffset += ALIGNMENTSIZE;
            }
            _currentPointer = _initalOffset;

            var headNodeSize = SkipListNode64.CalculateSizeNeeded(maxHeight, 0);
            AllocateNode(headNodeSize, out var memory);
            _ = new SkipListNode64(memory, ALIGNMENTSIZE, maxHeight, Array.Empty<byte>());
        }

        public override SkipListNode64 HeadNode => GetNode(_initalOffset);

        public override long AllocateValue(ReadOnlySpan<byte> value)
        {
            var length = value.Length;
            var alignedLength = AlignLength(length + 4);
            var currentSize = Interlocked.Add(ref _currentPointer, alignedLength);
            if (currentSize > _maxSize)
            {
                return 0;
            }
            currentSize -= alignedLength;

            ref var pointToWrite = ref _data[currentSize];
            Unsafe.WriteUnaligned(ref pointToWrite, length);
            value.CopyTo(new Span<byte>(_data, (int)currentSize + 4, length));
            return currentSize;
        }

        public override SkipListNode64 GetNode(long nodeLocation)
        {
            if (nodeLocation == 0) return default;
            ref var sizeOffset = ref _data[nodeLocation];
            var length = Unsafe.ReadUnaligned<int>(ref sizeOffset);
            var node = new SkipListNode64(new Span<byte>(_data, (int)nodeLocation, length), nodeLocation);
            return node;
        }

        public override ReadOnlySpan<byte> GetValue(long valueLocation)
        {
            ref var sizeOffset = ref _data[valueLocation];
            var length = Unsafe.ReadUnaligned<int>(ref sizeOffset);
            return new Span<byte>(_data, (int)valueLocation + sizeof(int), length);
        }

        protected override long AllocateNode(int length, out Span<byte> memory)
        {
            var alignedLength = AlignLength(length);
            var currentSize = Interlocked.Add(ref _currentPointer, alignedLength);
            if (currentSize > _maxSize)
            {
                memory = default;
                return 0;
            }

            currentSize -= alignedLength;
            memory = new Span<byte>(_data, (int)currentSize, length);
            return currentSize;
        }

        protected override void Dispose(bool isDisposing) { }
    }
}
