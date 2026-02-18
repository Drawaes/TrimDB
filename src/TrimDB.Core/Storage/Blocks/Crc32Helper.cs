using System;
using System.Buffers.Binary;

namespace TrimDB.Core.Storage.Blocks
{
    internal static class Crc32Helper
    {
        internal static uint Compute(ReadOnlySpan<byte> data)
        {
            Span<byte> hash = stackalloc byte[4];
            System.IO.Hashing.Crc32.Hash(data, hash);
            return BinaryPrimitives.ReadUInt32LittleEndian(hash);
        }
    }
}
