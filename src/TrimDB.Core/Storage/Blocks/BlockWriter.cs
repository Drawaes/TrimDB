using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage.Filters;

namespace TrimDB.Core.Storage.Blocks
{
    public class BlockWriter : IDisposable
    {
        private IEnumerator<IMemoryItem> _iterator;
        private bool _moreToWrite = true;
        private Filter _filter;
        private int _count;
        private ReadOnlyMemory<byte> _firstKey;
        private ReadOnlyMemory<byte> _lastKey;

        public BlockWriter(IEnumerator<IMemoryItem> iterator, Filter filter)
        {
            _firstKey = iterator.Current.Key.ToArray();
            _filter = filter;
            _iterator = iterator;

        }

        public bool MoreToWrite => _moreToWrite;
        public int Count => _count;
        public ReadOnlyMemory<byte> FirstKey => _firstKey;
        public ReadOnlyMemory<byte> LastKey => _lastKey;

        public void Dispose() => _iterator.Dispose();

        public void WriteBlock(Span<byte> memoryToFill)
        {
            var lastKey = new ReadOnlySpan<byte>();
            var span = memoryToFill;
            do
            {
                var key = _iterator.Current.Key;
                var value = _iterator.Current.Value;

                var lengthNeeded = (sizeof(int) * 2) + key.Length + value.Length;
                if (span.Length < lengthNeeded)
                {
                    span.Fill(0);
                    _moreToWrite = true;
                    return;
                }

                _count++;
                _filter.AddKey(key);
                BinaryPrimitives.WriteInt32LittleEndian(span, key.Length);
                span = span[sizeof(int)..];
                key.CopyTo(span);
                span = span[key.Length..];

                BinaryPrimitives.WriteInt32LittleEndian(span, value.Length);
                span = span[sizeof(int)..];
                value.CopyTo(span);
                lastKey = key;
                span = span[value.Length..];

            } while (_iterator.MoveNext());

            span.Fill(0);
            _lastKey = lastKey.ToArray();
            _moreToWrite = false;

        }
    }
}
