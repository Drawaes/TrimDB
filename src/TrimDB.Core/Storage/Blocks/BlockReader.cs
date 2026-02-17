using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace TrimDB.Core.Storage.Blocks
{
    public class BlockReader : IDisposable
    {
        private readonly IMemoryOwner<byte> _owner;
        private readonly ReadOnlyMemory<byte> _blockData;

        // Slotted block state
        private readonly int _itemCount;
        private int _currentSlot;

        // Current item state (set by navigation methods)
        private int _keyOffset;
        private int _keyLength;
        private int _valueOffset;
        private int _valueLength;
        private bool _isDeleted;

        public bool IsDeleted => _isDeleted;

        public BlockReader(IMemoryOwner<byte> owner)
        {
            _owner = owner;
            _blockData = owner.Memory;
            _itemCount = SlottedBlock.GetItemCount(_blockData.Span);
            _currentSlot = -1;
        }

        public ReadOnlySpan<byte> GetCurrentKey()
        {
            return _blockData.Span.Slice(_keyOffset, _keyLength);
        }

        public ReadOnlyMemory<byte> GetCurrentValue()
        {
            if (_isDeleted) return ReadOnlyMemory<byte>.Empty;
            return _blockData.Slice(_valueOffset, _valueLength);
        }

        /// <summary>
        /// Advance to the next entry in sorted order. Returns the key.
        /// Used for sequential iteration.
        /// </summary>
        public bool TryGetNextKey(out ReadOnlySpan<byte> key)
        {
            _currentSlot++;
            if (_currentSlot >= _itemCount)
            {
                key = default;
                return false;
            }

            LoadSlot(_currentSlot);
            key = _blockData.Span.Slice(_keyOffset, _keyLength);
            return true;
        }

        // Keep legacy method signatures for compatibility; they all delegate to TryGetNextKey
        public bool TryGetNextKeySlow(out ReadOnlySpan<byte> key) => TryGetNextKey(out key);
        public bool TryGetNextKey2(out ReadOnlySpan<byte> key) => TryGetNextKey(out key);

        /// <summary>
        /// Seek to the last entry in the block.
        /// </summary>
        public void GetLastKey()
        {
            if (_itemCount == 0) return;
            _currentSlot = _itemCount - 1;
            LoadSlot(_currentSlot);
        }

        /// <summary>
        /// Binary search for a key within this block. O(log n) comparisons.
        /// </summary>
        public KeySearchResult TryFindKey(ReadOnlySpan<byte> key)
        {
            if (_itemCount == 0) return KeySearchResult.NotFound;

            var span = _blockData.Span;
            var searchHash = SlottedBlock.ComputeKeyHash(key);

            int lo = 0, hi = _itemCount - 1;

            while (lo <= hi)
            {
                var mid = lo + ((hi - lo) >> 1);

                SlottedBlock.ReadSlot(span, mid, out var dataOffset, out var slotHash);
                SlottedBlock.ReadItemHeader(span, dataOffset, out var keyLen, out _);
                var midKey = SlottedBlock.GetKey(span, dataOffset, keyLen);

                var cmp = midKey.SequenceCompareTo(key);

                if (cmp < 0)
                {
                    lo = mid + 1;
                }
                else if (cmp > 0)
                {
                    hi = mid - 1;
                }
                else
                {
                    // Exact match
                    LoadSlot(mid);
                    _currentSlot = mid;
                    return KeySearchResult.Found;
                }
            }

            // Not found — determine position relative to block range
            if (lo == 0)
            {
                // Key is before the first entry in this block
                return KeySearchResult.Before;
            }
            if (lo >= _itemCount)
            {
                // Key is after the last entry in this block
                return KeySearchResult.After;
            }

            return KeySearchResult.NotFound;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LoadSlot(int slotIndex)
        {
            var span = _blockData.Span;
            SlottedBlock.ReadSlot(span, slotIndex, out var dataOffset, out var keyHash);
            SlottedBlock.ReadItemHeader(span, dataOffset, out var keyLen, out var valueLen);

            _isDeleted = valueLen == SlottedBlock.DeletedSentinel;
            _keyLength = keyLen;
            _keyOffset = dataOffset + SlottedBlock.ItemHeaderSize;
            _valueLength = _isDeleted ? 0 : valueLen;
            _valueOffset = _keyOffset + _keyLength;
        }

        public void Dispose() => _owner.Dispose();

        public enum KeySearchResult
        {
            Found,
            Before,
            After,
            NotFound
        }
    }
}
