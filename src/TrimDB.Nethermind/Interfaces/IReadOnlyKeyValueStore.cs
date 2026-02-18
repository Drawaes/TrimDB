using System;
using TrimDB.Core;

namespace TrimDB.Nethermind.Interfaces
{
    public interface IReadOnlyKeyValueStore
    {
        byte[]? Get(ReadOnlySpan<byte> key, ReadFlags flags = ReadFlags.None);
        bool KeyExists(ReadOnlySpan<byte> key);
        Span<byte> GetSpan(ReadOnlySpan<byte> key, ReadFlags flags = ReadFlags.None);
        void DangerousReleaseMemory(in ReadOnlySpan<byte> span);
    }
}
