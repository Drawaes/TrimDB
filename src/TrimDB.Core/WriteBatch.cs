using System;
using System.Collections.Generic;

namespace TrimDB.Core
{
    public sealed class WriteBatch
    {
        private readonly List<(Memory<byte> Key, Memory<byte> Value, bool Deleted)> _ops = new();

        public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
            => _ops.Add((key.ToArray(), value.ToArray(), false));

        public void Delete(ReadOnlySpan<byte> key)
            => _ops.Add((key.ToArray(), Memory<byte>.Empty, true));

        internal IReadOnlyList<(Memory<byte> Key, Memory<byte> Value, bool Deleted)> Operations => _ops;
    }
}
