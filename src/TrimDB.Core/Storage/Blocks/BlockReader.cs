using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace TrimDB.Core.Storage.Blocks
{
    public class BlockReader : IDisposable
    {
        private readonly IMemoryOwner<byte> _owner;
        private readonly ReadOnlyMemory<byte> _blockData;
        private long _location;
        private long _valueLocation;
        private int _valueLength;
        private int _keyLength;
        private int _keyLocation;
        private bool _isDeleted;

        public bool IsDeleted => _isDeleted;

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

        public bool TryGetNextKeySlow(out ReadOnlySpan<byte> key)
        {
            var span = _blockData.Span.Slice((int)_location);
            if (span.Length < sizeof(int) * 2)
            {
                key = default;
                return false;
            }

            _keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
            if (_keyLength == 0)
            {
                key = default;
                return false;
            }
            span = span.Slice(sizeof(int));
            _keyLocation = (int)_location + sizeof(int);
            key = span.Slice(0, _keyLength);
            span = span.Slice(_keyLength);

            _valueLength = BinaryPrimitives.ReadInt32LittleEndian(span);
            _valueLocation = _keyLocation + _keyLength + sizeof(int);

            _location = _valueLocation + _valueLength;

            return true;
        }

        public bool TryGetNextKey2(out ReadOnlySpan<byte> key)
        {
            var remaining = _blockData.Span.Length - _location;

            if (remaining < sizeof(int) * 2)
            {
                key = default;
                return false;
            }

            ref var ptr = ref MemoryMarshal.GetReference(_blockData.Span);
            ptr = ref Unsafe.Add(ref ptr, (int)_location);

            _keyLength = Unsafe.As<byte, int>(ref ptr);
            if (_keyLength == 0)
            {
                key = default;
                return false;
            }

            _keyLocation = (int)_location + sizeof(int);
            key = _blockData.Span.Slice(_keyLocation, _keyLength);

            remaining -= (sizeof(int) + _keyLength);

            if (remaining < sizeof(int)) return false;

            _valueLength = Unsafe.As<byte, int>(ref Unsafe.Add(ref ptr, sizeof(int) + _keyLength));
            _valueLocation = _keyLocation + _keyLength + sizeof(int);

            _location = _valueLocation + _valueLength;

            return true;
        }

        public bool TryGetNextKey(out ReadOnlySpan<byte> key)
        {
            var span = _blockData.Span;
            var remaining = span.Length - _location;

            if (remaining < sizeof(int) * 2)
            {
                key = default;
                return false;
            }

            ref var ptr = ref MemoryMarshal.GetReference(span);
            ptr = ref Unsafe.Add(ref ptr, (int)_location);

            _keyLength = Unsafe.As<byte, int>(ref ptr);
            if (_keyLength == 0)
            {
                key = default;
                return false;
            }

            _keyLocation = (int)_location + sizeof(int);
            key = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref ptr, sizeof(int)), _keyLength);

            remaining -= (sizeof(int) + _keyLength);

            if (remaining < sizeof(int)) return false;

            _valueLength = Unsafe.As<byte, int>(ref Unsafe.Add(ref ptr, sizeof(int) + _keyLength));
            if(_valueLength < 0)
            {
                _valueLength = 0;
                _isDeleted = true;
            }
            _valueLocation = _keyLocation + _keyLength + sizeof(int);

            _location = _valueLocation + _valueLength;

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
