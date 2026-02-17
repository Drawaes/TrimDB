using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace TrimDB.Core.Storage.Blocks
{
    /// <summary>
    /// StructLayout overlay for the slotted block header.
    /// Read with MemoryMarshal.Read&lt;BlockHeader&gt; — one read instead of N.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 4)]
    internal readonly struct BlockHeader
    {
        [FieldOffset(0)] public readonly ushort ItemCount;
        [FieldOffset(2)] public readonly ushort DataRegionStart;
    }

    /// <summary>
    /// StructLayout overlay for a single slot directory entry.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 4)]
    internal readonly struct SlotEntry
    {
        [FieldOffset(0)] public readonly ushort DataOffset;
        [FieldOffset(2)] public readonly ushort KeyHash;
    }

    /// <summary>
    /// StructLayout overlay for the item data header.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 4)]
    internal readonly struct ItemHeader
    {
        [FieldOffset(0)] public readonly ushort KeyLength;
        [FieldOffset(2)] public readonly ushort ValueLength;
    }

    /// <summary>
    /// Zero-allocation ref struct view over a slotted block's raw bytes.
    /// Supports binary search for point lookups and sequential iteration.
    /// Cannot cross await boundaries — use <see cref="BlockReader"/> for async contexts.
    /// </summary>
    public ref struct BlockView
    {
        private readonly ReadOnlySpan<byte> _block;
        private readonly int _itemCount;
        private int _currentSlot;

        // Current item state
        private int _keyOffset;
        private int _keyLength;
        private int _valueOffset;
        private int _valueLength;
        private bool _isDeleted;

        public BlockView(ReadOnlySpan<byte> block)
        {
            _block = block;
            var header = MemoryMarshal.Read<BlockHeader>(block);
            _itemCount = header.ItemCount;
            _currentSlot = -1;
            _keyOffset = 0;
            _keyLength = 0;
            _valueOffset = 0;
            _valueLength = 0;
            _isDeleted = false;
        }

        public int ItemCount => _itemCount;
        public bool IsDeleted => _isDeleted;

        public ReadOnlySpan<byte> CurrentKey
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _block.Slice(_keyOffset, _keyLength);
        }

        public ReadOnlySpan<byte> CurrentValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _isDeleted ? ReadOnlySpan<byte>.Empty : _block.Slice(_valueOffset, _valueLength);
        }

        /// <summary>
        /// Advance to the next entry. Returns the key span.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNextKey(out ReadOnlySpan<byte> key)
        {
            _currentSlot++;
            if (_currentSlot >= _itemCount)
            {
                key = default;
                return false;
            }
            LoadSlot(_currentSlot);
            key = _block.Slice(_keyOffset, _keyLength);
            return true;
        }

        /// <summary>
        /// Seek to the last entry.
        /// </summary>
        public void SeekToLast()
        {
            if (_itemCount == 0) return;
            _currentSlot = _itemCount - 1;
            LoadSlot(_currentSlot);
        }

        /// <summary>
        /// Binary search for a key. O(log n) comparisons.
        /// Uses StructLayout overlays for zero-copy slot/header reads.
        /// </summary>
        public BlockReader.KeySearchResult TryFindKey(ReadOnlySpan<byte> key)
        {
            if (_itemCount == 0) return BlockReader.KeySearchResult.NotFound;

            int lo = 0, hi = _itemCount - 1;

            while (lo <= hi)
            {
                var mid = lo + ((hi - lo) >> 1);

                var slot = ReadSlotEntry(mid);
                var itemHdr = MemoryMarshal.Read<ItemHeader>(_block[slot.DataOffset..]);
                var midKey = _block.Slice(slot.DataOffset + SlottedBlock.ItemHeaderSize, itemHdr.KeyLength);

                var cmp = midKey.SequenceCompareTo(key);

                if (cmp < 0)
                    lo = mid + 1;
                else if (cmp > 0)
                    hi = mid - 1;
                else
                {
                    LoadSlot(mid);
                    _currentSlot = mid;
                    return BlockReader.KeySearchResult.Found;
                }
            }

            if (lo == 0) return BlockReader.KeySearchResult.Before;
            if (lo >= _itemCount) return BlockReader.KeySearchResult.After;
            return BlockReader.KeySearchResult.NotFound;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private SlotEntry ReadSlotEntry(int index)
        {
            var offset = SlottedBlock.GetSlotOffset(index);
            return MemoryMarshal.Read<SlotEntry>(_block[offset..]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LoadSlot(int slotIndex)
        {
            var slot = ReadSlotEntry(slotIndex);
            var itemHdr = MemoryMarshal.Read<ItemHeader>(_block[slot.DataOffset..]);

            _isDeleted = itemHdr.ValueLength == SlottedBlock.DeletedSentinel;
            _keyLength = itemHdr.KeyLength;
            _keyOffset = slot.DataOffset + SlottedBlock.ItemHeaderSize;
            _valueLength = _isDeleted ? 0 : itemHdr.ValueLength;
            _valueOffset = _keyOffset + _keyLength;
        }
    }
}
