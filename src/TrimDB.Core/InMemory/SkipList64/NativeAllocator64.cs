using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading;
using static TrimDB.Core.Interop.Windows.Memory;

#pragma warning disable CS0618 // Obsolete SkipList64 types reference each other

namespace TrimDB.Core.InMemory.SkipList64
{
    [SupportedOSPlatform("windows")]
    [Obsolete("Use SkipList32 instead. Will be removed in a future release.")]
    public class NativeAllocator64 : SkipListAllocator64
    {
        private readonly IntPtr _pointer;
        private long _currentPointer;
        private readonly long _maxSize;

        public override SkipListNode64 HeadNode => GetNode(ALIGNMENTSIZE);

        public NativeAllocator64(long maxSize, byte maxHeight)
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

            var headNodeSize = SkipListNode64.CalculateSizeNeeded(maxHeight, 0);
            AllocateNode(headNodeSize, out var memory);
            _ = new SkipListNode64(memory, ALIGNMENTSIZE, maxHeight, Array.Empty<byte>());
        }

        public unsafe override SkipListNode64 GetNode(long nodeLocation)
        {
            if (nodeLocation == 0) return default;
            var ptr = (byte*)_pointer.ToPointer() + nodeLocation;
            var length = Unsafe.Read<int>(ptr);
            var node = new SkipListNode64(new Span<byte>(ptr, length), nodeLocation);
            return node;
        }

        public unsafe override ReadOnlySpan<byte> GetValue(long valueLocation)
        {
            var ptr = (byte*)_pointer.ToPointer() + valueLocation;
            var length = Unsafe.Read<int>(ptr);
            return new Span<byte>(ptr + sizeof(int), length);
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
