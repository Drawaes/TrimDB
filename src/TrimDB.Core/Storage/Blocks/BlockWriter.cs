using System;
using System.Collections.Generic;
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
            var builder = new SlottedBlockBuilder(memoryToFill);
            var lastKey = new ReadOnlySpan<byte>();

            do
            {
                var key = _iterator.Current.Key;
                var value = _iterator.Current.Value;
                var isDeleted = _iterator.Current.IsDeleted;

                if (!builder.TryAdd(key, value, isDeleted))
                {
                    if (builder.Count == 0)
                    {
                        // Entry too large for any block. Give up instead of looping forever.
                        _moreToWrite = false;
                        builder.Finish();
                        return;
                    }
                    _moreToWrite = true;
                    builder.Finish();
                    _count += builder.Count;
                    return;
                }

                _filter.AddKey(key);
                lastKey = key;

            } while (_iterator.MoveNext());

            _lastKey = lastKey.ToArray();
            _moreToWrite = false;
            _count += builder.Count;
            builder.Finish();
        }
    }
}
