using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.SkipList
{
    public abstract class SkipListAllocator : IDisposable
    {
        protected readonly SkipListHeighGenerator _heightGenerator;

        protected SkipListAllocator(byte maxHeight) => _heightGenerator = new SkipListHeighGenerator(maxHeight);

        public byte MaxHeight => _heightGenerator.MaxHeight;
        public byte CurrentHeight => _heightGenerator.CurrentHeight;

        public abstract SkipListNode HeadNode { get; }
        public abstract SkipListNode GetNode(long nodeLocation);
        public abstract ReadOnlySpan<byte> GetValue(long valueLocation);
        protected abstract long AllocateNode(int length, out Span<byte> memoy);
        public abstract long AllocateValue(ReadOnlySpan<byte> value);
        protected abstract void Dispose(bool isDisposing);

        public SkipListNode AllocateNode(ReadOnlySpan<byte> key)
        {
            var height = _heightGenerator.GetHeight();
            var memoryNeeded = SkipListNode.CalculateSizeNeeded(height, key.Length);
            var nodeLocation = AllocateNode(memoryNeeded, out var memory);

            if (nodeLocation == 0) return new SkipListNode();

            var returnValue = new SkipListNode(memory, nodeLocation, height, key);
            return returnValue;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SkipListAllocator()
        {
            Dispose(false);
        }
    }
}
