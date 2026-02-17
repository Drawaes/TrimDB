using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Filters;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class BlockEncodingFacts
    {
        /// <summary>
        /// Test #65: Write 10 items via BlockWriter, read them back via BlockReader.
        /// All keys and values must round-trip.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void BlockWriterOutputReadableByBlockReader()
        {
            var items = new List<FakeMemoryItem>();
            for (var i = 0; i < 10; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i:D4}");
                items.Add(new FakeMemoryItem(key, value));
            }

            items.Sort((a, b) => a.Key.SequenceCompareTo(b.Key));

            var block = new byte[FileConsts.PageSize];
            using var enumerator = items.GetEnumerator();
            enumerator.MoveNext();

            var filter = new NoOpFilter();
            using var writer = new BlockWriter(enumerator, filter);
            writer.WriteBlock(block);

            var reader = new BlockReader(new ByteArrayMemoryOwner(block));
            var readItems = new List<(string Key, string Value)>();
            while (reader.TryGetNextKey(out var key))
            {
                var value = reader.GetCurrentValue();
                readItems.Add((Encoding.UTF8.GetString(key), Encoding.UTF8.GetString(value.Span)));
            }

            Assert.Equal(10, readItems.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.Equal($"key{i:D4}", readItems[i].Key);
                Assert.Equal($"value{i:D4}", readItems[i].Value);
            }
        }

        /// <summary>
        /// Test #66: Fill a block to near PageSize, verify spill to next block.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void BlockWriterHandlesMaxCapacityBlock()
        {
            // Each item: 4 (keyLen) + keyBytes + 4 (valLen) + valBytes
            // Key = "k" + 3 digits = 4 bytes, value = "v" + 3 digits = 4 bytes
            // Per item = 4 + 4 + 4 + 4 = 16 bytes
            // PageSize = 4096 => fits 256 items exactly, but sentinel needs 0 bytes at end
            // Use enough items to fill two blocks
            var items = new List<FakeMemoryItem>();
            for (var i = 0; i < 300; i++)
            {
                var key = Encoding.UTF8.GetBytes($"k{i:D3}");
                var value = Encoding.UTF8.GetBytes($"v{i:D3}");
                items.Add(new FakeMemoryItem(key, value));
            }

            items.Sort((a, b) => a.Key.SequenceCompareTo(b.Key));

            using var enumerator = items.GetEnumerator();
            enumerator.MoveNext();

            var filter = new NoOpFilter();
            using var writer = new BlockWriter(enumerator, filter);

            // Write first block
            var block1 = new byte[FileConsts.PageSize];
            writer.WriteBlock(block1);
            Assert.True(writer.MoreToWrite, "Should have items spilling to a second block");

            // Write second block
            var block2 = new byte[FileConsts.PageSize];
            writer.WriteBlock(block2);

            // Read both blocks and count total items
            var count1 = CountItemsInBlock(block1);
            var count2 = CountItemsInBlock(block2);

            Assert.True(count1 > 0, "First block should have items");
            Assert.True(count2 > 0, "Second block should have items");
            Assert.Equal(300, count1 + count2);
        }

        /// <summary>
        /// Test #67: Key+value+headers exactly fills a block. Next item goes to next block.
        /// Slotted format: header(4) + slot(4) + itemHeader(4) + key + value = PageSize
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void KeyValuePairExactlyFillsBlock()
        {
            // Slotted block overhead for a single item:
            //   header: 4 bytes (count + dataRegionStart)
            //   slot:   4 bytes (dataOffset + keyHash)
            //   item header: 4 bytes (keyLen:2 + valueLen:2)
            //   Total overhead = 12
            // So: 12 + keyBytes + valBytes = PageSize
            var keySize = 8;
            var valueSize = FileConsts.PageSize - 12 - keySize;

            var key1 = new byte[keySize];
            Encoding.UTF8.GetBytes("aaaaaaaa").CopyTo(key1, 0);
            var value1 = new byte[valueSize];
            Array.Fill(value1, (byte)'X');

            var key2 = Encoding.UTF8.GetBytes("bbbbbbbb");
            var value2 = Encoding.UTF8.GetBytes("small");

            var items = new List<FakeMemoryItem>
            {
                new(key1, value1),
                new(key2, value2),
            };

            using var enumerator = items.GetEnumerator();
            enumerator.MoveNext();

            var filter = new NoOpFilter();
            using var writer = new BlockWriter(enumerator, filter);

            var block1 = new byte[FileConsts.PageSize];
            writer.WriteBlock(block1);

            // The first item should exactly fill the block, so there should be more to write
            Assert.True(writer.MoreToWrite);

            var block2 = new byte[FileConsts.PageSize];
            writer.WriteBlock(block2);

            Assert.Equal(1, CountItemsInBlock(block1));
            Assert.Equal(1, CountItemsInBlock(block2));
        }

        /// <summary>
        /// Test #68: Empty key (zero-length) collides with block sentinel.
        /// BUG-A: BlockReader treats keyLength==0 as end-of-block.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public void EmptyKeyInBlockBecomesInvisible()
        {
            var items = new List<FakeMemoryItem>
            {
                new(Array.Empty<byte>(), Encoding.UTF8.GetBytes("value-for-empty-key")),
                new(Encoding.UTF8.GetBytes("normalkey"), Encoding.UTF8.GetBytes("normalvalue")),
            };

            // Empty key sorts before "normalkey" in byte ordering, so it's first
            items.Sort((a, b) => a.Key.SequenceCompareTo(b.Key));

            using var enumerator = items.GetEnumerator();
            enumerator.MoveNext();

            var filter = new NoOpFilter();
            using var writer = new BlockWriter(enumerator, filter);

            var block = new byte[FileConsts.PageSize];
            writer.WriteBlock(block);

            // Now try to read the block
            var reader = new BlockReader(new ByteArrayMemoryOwner(block));
            var readCount = 0;
            var foundEmptyKey = false;
            while (reader.TryGetNextKey(out var key))
            {
                readCount++;
                if (key.Length == 0) foundEmptyKey = true;
            }

            // BUG-A: The empty key (keyLength==0) is treated as end-of-block sentinel.
            // This assertion verifies the bug exists. When fixed, the empty key should be readable.
            // For now, we expect the empty key to be invisible and the reader to stop early.
            Assert.True(foundEmptyKey, "Empty key should be readable (currently invisible due to BUG-A)");
            Assert.Equal(2, readCount);
        }

        /// <summary>
        /// Test #69: Tombstone encoding consistency across BlockWriter and BlockReader.
        /// Slotted format: tombstones use valueLen=0xFFFF in the item header.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public void TombstoneEncodingConsistencyAcrossWriters()
        {
            // Write a tombstone entry via BlockWriter
            var key = Encoding.UTF8.GetBytes("deletedkey");
            var tombstoneItem = new FakeMemoryItem(key, Array.Empty<byte>(), isDeleted: true);

            var items = new List<FakeMemoryItem> { tombstoneItem };
            using var enumerator = items.GetEnumerator();
            enumerator.MoveNext();

            var filter = new NoOpFilter();
            using var writer = new BlockWriter(enumerator, filter);

            var block = new byte[FileConsts.PageSize];
            writer.WriteBlock(block);

            // Slotted format: header(4) + slot[0](4) then item data at the slot's offset
            // Read slot 0 to find the item data offset
            var itemCount = BinaryPrimitives.ReadUInt16LittleEndian(block.AsSpan(0));
            Assert.Equal(1, itemCount);

            var dataOffset = BinaryPrimitives.ReadUInt16LittleEndian(block.AsSpan(4));
            var writtenKeyLen = BinaryPrimitives.ReadUInt16LittleEndian(block.AsSpan(dataOffset));
            var writtenValueLen = BinaryPrimitives.ReadUInt16LittleEndian(block.AsSpan(dataOffset + 2));

            Assert.Equal(key.Length, writtenKeyLen);
            Assert.Equal(0xFFFF, writtenValueLen); // Tombstone sentinel

            // Also verify via BlockReader that IsDeleted is set
            var reader = new BlockReader(new ByteArrayMemoryOwner(block));
            Assert.True(reader.TryGetNextKey(out _));
            Assert.True(reader.IsDeleted, "Tombstone written by BlockWriter should be detected as deleted by BlockReader");
        }

        // --- Helpers ---

        private static int CountItemsInBlock(byte[] block)
        {
            var reader = new BlockReader(new ByteArrayMemoryOwner(block));
            var count = 0;
            while (reader.TryGetNextKey(out _))
            {
                count++;
            }
            return count;
        }

        internal class FakeMemoryItem : IMemoryItem
        {
            private readonly byte[] _key;
            private readonly byte[] _value;
            private readonly bool _isDeleted;

            public FakeMemoryItem(byte[] key, byte[] value, bool isDeleted = false)
            {
                _key = key;
                _value = value;
                _isDeleted = isDeleted;
            }

            public ReadOnlySpan<byte> Key => _key;
            public ReadOnlySpan<byte> Value => _value;
            public bool IsDeleted => _isDeleted;
        }

        internal class ByteArrayMemoryOwner : IMemoryOwner<byte>
        {
            private readonly byte[] _array;

            public ByteArrayMemoryOwner(byte[] array) => _array = array;

            public Memory<byte> Memory => _array;

            public void Dispose() { }
        }

        internal class NoOpFilter : Filter
        {
            public override bool MayContainKey(long hashedValue) => true;
            public override bool AddKey(ReadOnlySpan<byte> key) => true;
            public override int WriteToPipe(System.IO.Pipelines.PipeWriter pipeWriter) => 0;
            public override void LoadFromBlock(ReadOnlyMemory<byte> memory) { }
        }
    }
}
