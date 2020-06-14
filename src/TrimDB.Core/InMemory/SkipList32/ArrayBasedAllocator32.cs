using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace TrimDB.Core.InMemory.SkipList32
{
    public class ArrayBasedAllocator32 : IDisposable
    {
        private readonly GCHandle _gcHandle;
        private byte[] _data;
        private int _initalOffset;
        private int _currentPointer;
        protected const int ALIGNMENTSIZE = 64;
        private readonly int _maxSize;
        private readonly SkipList64.SkipListHeightGenerator64 _heightGenerator;

        public SkipListNode32 HeadNode => GetNode(_initalOffset);

        public byte CurrentHeight => _heightGenerator.CurrentHeight;
        public byte MaxHeight => _heightGenerator.MaxHeight;

        public ArrayBasedAllocator32(int maxSize, byte maxHeight)
        {
            _heightGenerator = new SkipList64.SkipListHeightGenerator64(maxHeight);
            _maxSize = maxSize;

            _data = new byte[maxSize + ALIGNMENTSIZE - 1];
            _gcHandle = GCHandle.Alloc(_data, GCHandleType.Pinned);
            _initalOffset = (int)(AlignLength(_gcHandle.AddrOfPinnedObject().ToInt64()) - _gcHandle.AddrOfPinnedObject().ToInt64());
            if (_initalOffset == 0)
            {
                _initalOffset += ALIGNMENTSIZE;
            }
            _currentPointer = _initalOffset;

            var headNodeSize = SkipListNode32.CalculateSizeNeeded(maxHeight, 0);
            AllocateNode(headNodeSize, out var memory);
            _ = new SkipListNode32(memory, ALIGNMENTSIZE, maxHeight, Array.Empty<byte>());
        }

        public unsafe SkipListNode32 GetNode(int nodeLocation)
        {
            if (nodeLocation == 0) return default;
            ref var sizeOffset = ref _data[nodeLocation];
            var length = Unsafe.ReadUnaligned<int>(ref sizeOffset);
            var node = new SkipListNode32(new Span<byte>(_data, nodeLocation, length), nodeLocation);
            return node;
        }

        public unsafe ReadOnlySpan<byte> GetValue(int valueLocation)
        {
            ref var sizeOffset = ref _data[valueLocation];
            var length = Unsafe.ReadUnaligned<int>(ref sizeOffset);
            return new Span<byte>(_data, valueLocation + sizeof(int), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AlignLength(int length)
        {
            return (length + (ALIGNMENTSIZE - 1)) & ~(ALIGNMENTSIZE - 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long AlignLength(long length)
        {
            return (length + (ALIGNMENTSIZE - 1)) & ~(ALIGNMENTSIZE - 1);
        }

        public SkipListNode32 AllocateNode(ReadOnlySpan<byte> key)
        {
            var height = _heightGenerator.GetHeight();
            var memoryNeeded = SkipListNode32.CalculateSizeNeeded(height, key.Length);
            var nodeLocation = AllocateNode(memoryNeeded, out var memory);

            if (nodeLocation == 0) return new SkipListNode32();

            var returnValue = new SkipListNode32(memory, nodeLocation, height, key);
            return returnValue;
        }

        public unsafe int AllocateValue(ReadOnlySpan<byte> value)
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
            value.CopyTo(new Span<byte>(_data, currentSize + 4, length));
            return currentSize;
        }

        private unsafe int AllocateNode(int length, out Span<byte> memory)
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

        public void Dispose() => _gcHandle.Free();
    }
}
