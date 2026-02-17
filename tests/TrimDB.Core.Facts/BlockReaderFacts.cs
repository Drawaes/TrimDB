using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using Xunit;

namespace TrimDB.Core.Facts
{
    public  class BlockReaderFacts
    {
        [Fact]
        public void CanReadFullBlock()
        {
            // Build a slotted block via BlockWriter from test items
            var items = new List<BlockEncodingFacts.FakeMemoryItem>();
            for (var i = 0; i < 20; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"VALUE={i:D4}");
                items.Add(new BlockEncodingFacts.FakeMemoryItem(key, value));
            }
            items.Sort((a, b) => a.Key.SequenceCompareTo(b.Key));

            var block = new byte[Storage.FileConsts.PageSize];
            using var enumerator = items.GetEnumerator();
            enumerator.MoveNext();

            var filter = new BlockEncodingFacts.NoOpFilter();
            using var writer = new BlockWriter(enumerator, filter);
            writer.WriteBlock(block);

            var bl = new BlockReader(new MemOwner(block));
            var count = 0;
            while (bl.TryGetNextKey(out var key))
            {
                var mem = bl.GetCurrentValue();
                count++;
            }
            Assert.Equal(20, count);
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
