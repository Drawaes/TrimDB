using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace TrimDB.Core.Storage
{
    public class BlockReader : IDisposable
    {
        private IMemoryOwner<byte> _owner;
        private ReadOnlyMemory<byte> _blockData;
        private long _location;
        private long _valueLocation;
        private int _valueLength;
        private int _keyLength;
        private int _keyLocation;

        public BlockReader(IMemoryOwner<byte> owner)
        {
            _owner = owner;
            _blockData = owner.Memory;
        }

        public ReadOnlySpan<byte> GetCurrentKey()
        {
            var span = _blockData.Span.Slice(_keyLocation, _keyLength);
            return span;
        }

        public bool TryGetNextKey(out ReadOnlySpan<byte> key)
        {
            var span = _blockData.Span.Slice((int)_location);
            if (span.Length < sizeof(long))
            {
                key = default;
                return false;
            }

            var sizeOfKV = BinaryPrimitives.ReadInt64LittleEndian(span);
            if (sizeOfKV == 0)
            {
                key = default;
                return false;
            }

            span = span[sizeof(long)..(int)sizeOfKV];

            _keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
            span = span.Slice(sizeof(int));

            _valueLength = span.Length - _keyLength;
            _valueLocation = _location + sizeof(long) + sizeof(int) + _keyLength;
            _keyLocation = (int)_location + sizeof(long) + sizeof(int);
            _location += sizeOfKV;

            key = span.Slice(0, _keyLength);
            return true;
        }

        public ReadOnlyMemory<byte> GetCurrentValue()
        {
            return _blockData.Slice((int)_valueLocation, _valueLength);
        }

        public void GetLastKey()
        {
            while (TryGetNextKey(out _))
            {
            }
        }

        public KeySearchResult TryFindKey(ReadOnlySpan<byte> key)
        {
            TryGetNextKey(out var nextKey);

            var compare = key.SequenceCompareTo(nextKey);
            if (compare < 0) return KeySearchResult.Before;
            if (compare == 0) return KeySearchResult.Found;

            while (TryGetNextKey(out nextKey))
            {
                compare = key.SequenceCompareTo(nextKey);
                if (compare == 0)
                {
                    return KeySearchResult.Found;
                }
                else if (compare < 0)
                {
                    return KeySearchResult.NotFound;
                }
            }

            return KeySearchResult.After;
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
