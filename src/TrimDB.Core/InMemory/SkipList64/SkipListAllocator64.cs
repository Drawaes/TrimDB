using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

#pragma warning disable CS0618 // Obsolete SkipList64 types reference each other

namespace TrimDB.Core.InMemory.SkipList64
{
    [Obsolete("Use SkipList32 instead. Will be removed in a future release.")]
    public abstract class SkipListAllocator64 : IDisposable
    {
        protected readonly SkipListHeightGenerator64 HeightGenerator;
        protected const int ALIGNMENTSIZE = 64;

        protected SkipListAllocator64(byte maxHeight) => HeightGenerator = new SkipListHeightGenerator64(maxHeight);

        public byte MaxHeight => HeightGenerator.MaxHeight;
        public byte CurrentHeight => HeightGenerator.CurrentHeight;

        public abstract SkipListNode64 HeadNode { get; }
        public abstract SkipListNode64 GetNode(long nodeLocation);
        public abstract ReadOnlySpan<byte> GetValue(long valueLocation);
        protected abstract long AllocateNode(int length, out Span<byte> memoy);
        public abstract long AllocateValue(ReadOnlySpan<byte> value);
        protected abstract void Dispose(bool isDisposing);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long AlignLength(long length)
        {
            return (length + (ALIGNMENTSIZE - 1)) & ~(ALIGNMENTSIZE - 1);
        }

        public SkipListNode64 AllocateNode(ReadOnlySpan<byte> key)
        {
            var height = HeightGenerator.GetHeight();
            var memoryNeeded = SkipListNode64.CalculateSizeNeeded(height, key.Length);
            var nodeLocation = AllocateNode(memoryNeeded, out var memory);

            if (nodeLocation == 0) return new SkipListNode64();

            var returnValue = new SkipListNode64(memory, nodeLocation, height, key);
            return returnValue;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SkipListAllocator64()
        {
            Dispose(false);
        }
    }
}
