using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace TrimDB.Core.Storage.Blocks
{
    /// <summary>
    /// Slotted block format for SSTable data blocks.
    ///
    /// Layout:
    /// ┌───────────────────────────────────────┐
    /// │ Header (4 bytes):                     │
    /// │   itemCount: uint16                   │
    /// │   dataRegionStart: uint16             │
    /// ├───────────────────────────────────────┤
    /// │ Slot directory (4 bytes per entry):   │
    /// │   [dataOffset:2][keyHash:2] per slot  │
    /// │   Slots stored in key-sorted order    │
    /// ├───────────────────────────────────────┤
    /// │ Free space (zeroed)                   │
    /// ├───────────────────────────────────────┤
    /// │ Item data (grows backward from end):  │
    /// │   [keyLen:2][valueLen:2][key][value]   │
    /// │   Deleted entries: valueLen = 0xFFFF  │
    /// └───────────────────────────────────────┘
    /// </summary>
    internal static class SlottedBlock
    {
        internal const int HeaderSize = 4;
        internal const int SlotSize = 4;
        internal const int ItemHeaderSize = 4; // keyLen(2) + valueLen(2)
        internal const ushort DeletedSentinel = 0xFFFF;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ushort ComputeKeyHash(ReadOnlySpan<byte> key)
        {
            // FNV-1a 32-bit, folded to 16-bit
            uint h = 2166136261u;
            foreach (var b in key)
            {
                h = (h ^ b) * 16777619u;
            }
            return (ushort)(h ^ (h >> 16));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetItemCount(ReadOnlySpan<byte> block)
        {
            return BinaryPrimitives.ReadUInt16LittleEndian(block);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetDataRegionStart(ReadOnlySpan<byte> block)
        {
            return BinaryPrimitives.ReadUInt16LittleEndian(block[2..]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetSlotOffset(int slotIndex)
        {
            return HeaderSize + (slotIndex * SlotSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ReadSlot(ReadOnlySpan<byte> block, int slotIndex, out ushort dataOffset, out ushort keyHash)
        {
            var slotStart = HeaderSize + (slotIndex * SlotSize);
            dataOffset = BinaryPrimitives.ReadUInt16LittleEndian(block[slotStart..]);
            keyHash = BinaryPrimitives.ReadUInt16LittleEndian(block[(slotStart + 2)..]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ReadItemHeader(ReadOnlySpan<byte> block, int dataOffset, out ushort keyLen, out ushort valueLen)
        {
            keyLen = BinaryPrimitives.ReadUInt16LittleEndian(block[dataOffset..]);
            valueLen = BinaryPrimitives.ReadUInt16LittleEndian(block[(dataOffset + 2)..]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ReadOnlySpan<byte> GetKey(ReadOnlySpan<byte> block, int dataOffset, int keyLen)
        {
            return block.Slice(dataOffset + ItemHeaderSize, keyLen);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ReadOnlySpan<byte> GetValue(ReadOnlySpan<byte> block, int dataOffset, int keyLen, int valueLen)
        {
            return block.Slice(dataOffset + ItemHeaderSize + keyLen, valueLen);
        }
    }

    /// <summary>
    /// Builds a slotted block incrementally. Items must be added in sorted key order.
    /// Slots grow forward from the header, item data grows backward from the end.
    /// </summary>
    internal ref struct SlottedBlockBuilder
    {
        private readonly Span<byte> _block;
        private int _slotEnd;      // byte offset after last slot written
        private int _dataStart;    // byte offset where next item data starts (grows backward)
        private int _count;

        public SlottedBlockBuilder(Span<byte> block)
        {
            _block = block;
            _slotEnd = SlottedBlock.HeaderSize;
            _dataStart = block.Length;
            _count = 0;
        }

        public int Count => _count;

        /// <summary>
        /// Returns the number of free bytes remaining for slots + item data.
        /// </summary>
        public int FreeSpace => _dataStart - _slotEnd;

        /// <summary>
        /// Calculates the total bytes needed to add an entry with the given key and value lengths.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SpaceNeeded(int keyLength, int valueLength, bool isDeleted)
        {
            var itemSize = SlottedBlock.ItemHeaderSize + keyLength + (isDeleted ? 0 : valueLength);
            return SlottedBlock.SlotSize + itemSize;
        }

        /// <summary>
        /// Try to add an entry to the block. Returns false if the block is full.
        /// Items MUST be added in sorted key order.
        /// </summary>
        public bool TryAdd(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isDeleted)
        {
            var valueBytes = isDeleted ? 0 : value.Length;
            var itemSize = SlottedBlock.ItemHeaderSize + key.Length + valueBytes;
            var totalNeeded = SlottedBlock.SlotSize + itemSize;

            if (_slotEnd + totalNeeded > _dataStart)
                return false;

            var keyHash = SlottedBlock.ComputeKeyHash(key);

            // Write item data backward from end
            _dataStart -= itemSize;
            BinaryPrimitives.WriteUInt16LittleEndian(_block[_dataStart..], (ushort)key.Length);
            BinaryPrimitives.WriteUInt16LittleEndian(_block[(_dataStart + 2)..],
                isDeleted ? SlottedBlock.DeletedSentinel : (ushort)value.Length);
            key.CopyTo(_block[(_dataStart + SlottedBlock.ItemHeaderSize)..]);
            if (!isDeleted && value.Length > 0)
                value.CopyTo(_block[(_dataStart + SlottedBlock.ItemHeaderSize + key.Length)..]);

            // Write slot entry forward
            BinaryPrimitives.WriteUInt16LittleEndian(_block[_slotEnd..], (ushort)_dataStart);
            BinaryPrimitives.WriteUInt16LittleEndian(_block[(_slotEnd + 2)..], keyHash);
            _slotEnd += SlottedBlock.SlotSize;

            _count++;
            return true;
        }

        /// <summary>
        /// Finalize the block: write the header and zero free space.
        /// Must be called after all entries are added.
        /// </summary>
        public void Finish()
        {
            BinaryPrimitives.WriteUInt16LittleEndian(_block, (ushort)_count);
            BinaryPrimitives.WriteUInt16LittleEndian(_block[2..], (ushort)_dataStart);
            // Zero the free space between slots and data
            _block[_slotEnd.._dataStart].Clear();
        }
    }
}
