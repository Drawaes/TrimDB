using System;

namespace TrimDB.Core
{
    public readonly struct ScanEntry
    {
        public readonly ReadOnlyMemory<byte> Key;
        public readonly ReadOnlyMemory<byte> Value;

        public ScanEntry(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
        {
            Key = key;
            Value = value;
        }
    }
}
