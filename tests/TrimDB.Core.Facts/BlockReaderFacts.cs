using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.Storage.Blocks;
using Xunit;

namespace TrimDB.Core.Facts
{
    public  class BlockReaderFacts
    {
        [Fact]
        public void CanReadFullBlock()
        {
            var bl = new BlockReader(new MemOwner(CommonData.SingleBlock));

            while(bl.TryGetNextKey(out var key))
            {
                var mem = bl.GetCurrentValue();
            }

        }

        public class MemOwner : MemoryManager<byte>
        {
            private byte[] _array;

            public MemOwner(byte[] array)
            {
                _array = array;
            }

            public override Span<byte> GetSpan() => _array;

            public override MemoryHandle Pin(int elementIndex = 0)
            {
                throw new NotImplementedException();
            }

            public override void Unpin()
            {
                throw new NotImplementedException();
            }

            protected override void Dispose(bool disposing)
            {
            }
        }
    }
}
