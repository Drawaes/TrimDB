using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TrimDB.Core.InMemory
{
    public abstract class MemoryTable : IEnumerable<IMemoryItem>
    {
        private long _walHighWatermark;

        public long WalHighWatermark => Volatile.Read(ref _walHighWatermark);

        public void UpdateWalHighWatermark(long offset)
        {
            while (true)
            {
                var current = Volatile.Read(ref _walHighWatermark);
                if (offset <= current) return;
                if (Interlocked.CompareExchange(ref _walHighWatermark, offset, current) == current) return;
            }
        }

        public abstract IEnumerator<IMemoryItem> GetEnumerator();
        public abstract bool Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
        public abstract bool Delete(ReadOnlySpan<byte> key);
        public abstract SearchResult TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
