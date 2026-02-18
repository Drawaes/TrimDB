using System;
using TrimDB.Core;

namespace TrimDB.Nethermind.Interfaces
{
    public interface IWriteOnlyKeyValueStore
    {
        void Set(ReadOnlySpan<byte> key, byte[]? value, WriteFlags flags = WriteFlags.None);
        void Remove(ReadOnlySpan<byte> key);
    }
}
