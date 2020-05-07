using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using static TrimDB.Core.Interop.Windows.Memory;

namespace TrimDB.Core.InMemory.SkipList32
{
    public class NativeAllocator32 : IDisposable
    {
        private readonly IntPtr _pointer;
        private int _currentPointer;
        private const int ALIGNMENTSIZE = 16;
        private readonly int _maxSize;
        private readonly SkipList64.SkipListHeightGenerator64 _heightGenerator;

        public SkipListNode32 HeadNode => GetNode(ALIGNMENTSIZE);

        public byte CurrentHeight => _heightGenerator.CurrentHeight;
        public byte MaxHeight => _heightGenerator.MaxHeight;

        public NativeAllocator32(int maxSize, byte maxHeight)
        {
            _heightGenerator = new SkipList64.SkipListHeightGenerator64(maxHeight);
            _maxSize = maxSize;
            _pointer = VirtualAlloc(IntPtr.Zero, (UIntPtr)maxSize, AllocationType.MEM_COMMIT | AllocationType.MEM_RESERVE, Protection.PAGE_READWRITE);
            if (_pointer == IntPtr.Zero)
            {
                var error = Marshal.GetLastWin32Error();
                throw new OutOfMemoryException($"We could not allocate memory for the skip list error code was {error}");
            }
            _currentPointer = ALIGNMENTSIZE;

            var headNodeSize = SkipListNode32.CalculateSizeNeeded(maxHeight, 0);
            AllocateNode(headNodeSize, out var memory);
            _ = new SkipListNode32(memory, ALIGNMENTSIZE, maxHeight, Array.Empty<byte>());
        }

        public unsafe SkipListNode32 GetNode(int nodeLocation)
        {
            if (nodeLocation == 0) return default;
            var ptr = (byte*)_pointer.ToPointer() + nodeLocation;
            var length = Unsafe.Read<int>(ptr);
            var node = new SkipListNode32(new Span<byte>(ptr, length), nodeLocation);
            return node;
        }

        public unsafe ReadOnlySpan<byte> GetValue(int valueLocation)
        {
            var ptr = (byte*)_pointer.ToPointer() + valueLocation;
            var length = Unsafe.Read<int>(ptr);
            return new Span<byte>(ptr + sizeof(int), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AlignLength(int length)
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

            var ptr = (byte*)_pointer.ToPointer() + currentSize;
            *(int*)ptr = value.Length;
            var span = new Span<byte>(ptr + 4, value.Length);
            value.CopyTo(span);
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
            memory = new Span<byte>((byte*)_pointer.ToPointer() + currentSize, length);
            return currentSize;
        }

        public void Dispose()
        {
            VirtualFree(_pointer, (UIntPtr)0, FreeType.MEM_RELEASE);
        }
    }
}
