using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.InMemory
{
    public abstract class MemoryTable : IEnumerable<IMemoryItem>
    {
        public abstract IEnumerator<IMemoryItem> GetEnumerator();
        public abstract bool Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
        public abstract SearchResult TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
