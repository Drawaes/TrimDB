using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using static TrimDB.Core.Interop.Windows.Memory;

namespace TrimDB.Core.SkipList
{
    public class NativeAllocator : SkipListAllocator
    {
        private IntPtr _pointer;
        private long _currentPointer;
        private const int ALIGNMENTSIZE = 64;
        private long _maxSize;

        public override SkipListNode HeadNode => GetNode(ALIGNMENTSIZE);

        public NativeAllocator(long maxSize, byte maxHeight)
            : base(maxHeight)
        {
            _maxSize = maxSize;
            _pointer = VirtualAlloc(IntPtr.Zero, (UIntPtr)maxSize, AllocationType.MEM_COMMIT | AllocationType.MEM_RESERVE, Protection.PAGE_READWRITE);
            if (_pointer == IntPtr.Zero)
            {
                var error = Marshal.GetLastWin32Error();
                throw new OutOfMemoryException($"We could not allocate memory for the skip list error code was {error}");
            }

            _currentPointer = ALIGNMENTSIZE;

            var headNodeSize = SkipListNode.CalculateSizeNeeded(maxHeight, 0);
            AllocateNode(headNodeSize, out var memory);
            _ = new SkipListNode(memory, ALIGNMENTSIZE, maxHeight, Array.Empty<byte>());
        }

        public unsafe override SkipListNode GetNode(long nodeLocation)
        {
            var ptr = (byte*)_pointer.ToPointer() + nodeLocation;
            var length = Unsafe.Read<int>(ptr);
            var node = new SkipListNode(new Span<byte>(ptr, length), nodeLocation);
            return node;
        }

        public unsafe override ReadOnlySpan<byte> GetValue(long valueLocation)
        {
            var ptr = (byte*)_pointer.ToPointer() + valueLocation;
            var length = Unsafe.Read<int>(ptr);
            return new Span<byte>(ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AlignLength(int length)
        {
            return (length + (ALIGNMENTSIZE - 1)) & ~(ALIGNMENTSIZE - 1);
        }

        protected unsafe override long AllocateNode(int length, out Span<byte> memory)
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

        public unsafe override long AllocateValue(ReadOnlySpan<byte> value)
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

        protected override void Dispose(bool isDisposing)
        {
            VirtualFree(_pointer, (UIntPtr)0, FreeType.MEM_RELEASE);
        }
    }
}
