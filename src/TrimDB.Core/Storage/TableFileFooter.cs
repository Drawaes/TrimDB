using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using TrimDB.Core.Storage.MetaData;

namespace TrimDB.Core.Storage
{
    public static class TableFileFooter
    {
        public static Span<byte> WriteTOCEntry(Span<byte> span, TableOfContentsEntryType tocType, long offset, int length)
        {
            var tocEntry = new TableOfContentsEntry() { EntryType = tocType, Offset = offset, Length = length };
            Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(span), tocEntry);
            return span[Unsafe.SizeOf<TableOfContentsEntry>()..];
        }

        public static void WriteTOC(PipeWriter filePipe, params TableOfContentsEntry[] tocEntries)
        {
            var tocSize = tocEntries.Length * Unsafe.SizeOf<TableOfContentsEntry>();
            tocSize += sizeof(uint) + sizeof(int) + sizeof(int);

            var span = filePipe.GetSpan(tocSize);
            span = span[..tocSize];
            var totalSpan = span;

            foreach (var te in tocEntries)
            {
                span = WriteTOCEntry(span, te.EntryType, te.Offset, te.Length);
            }

            BinaryPrimitives.WriteInt32LittleEndian(span, FileConsts.Version);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteInt32LittleEndian(span, tocSize);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteUInt32LittleEndian(span, FileConsts.MagicNumber);
            span = span[sizeof(uint)..];

            filePipe.Advance(tocSize);
        }

        public static int WriteStats(PipeWriter pipeWriter, ReadOnlySpan<byte> firstKey, ReadOnlySpan<byte> lastKey, int count)
        {
            var totalWritten = firstKey.Length + sizeof(int) * 3 + lastKey.Length;

            var memory = pipeWriter.GetSpan(totalWritten);
            var totalSpan = memory[..totalWritten];

            BinaryPrimitives.WriteInt32LittleEndian(memory, firstKey.Length);
            memory = memory[sizeof(int)..];
            firstKey.CopyTo(memory);
            memory = memory[firstKey.Length..];

            BinaryPrimitives.WriteInt32LittleEndian(memory, lastKey.Length);
            memory = memory[sizeof(int)..];
            lastKey.CopyTo(memory);
            memory = memory[lastKey.Length..];

            BinaryPrimitives.WriteInt32LittleEndian(memory, count);
            memory = memory[sizeof(int)..];

            pipeWriter.Advance(totalWritten);

            return totalWritten;
        }

        public static int WriteBlockOffsets(PipeWriter pipeWriter, List<long> blockOffsets)
        {
            var sizeToWrite = blockOffsets.Count * sizeof(long) + sizeof(int);

            var span = pipeWriter.GetSpan(sizeToWrite);
            var totalSpan = span[..sizeToWrite];

            BinaryPrimitives.WriteInt32LittleEndian(span, blockOffsets.Count);
            span = span[sizeof(int)..];

            foreach (var offset in blockOffsets)
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, offset);
                span = span[sizeof(long)..];
            }

            pipeWriter.Advance(sizeToWrite);
            return sizeToWrite;
        }
    }
}
