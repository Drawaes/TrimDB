using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Drawing;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.CompilerServices;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList64;
using TrimDB.Core.Storage.Filters;

namespace TrimDB.Core.Storage
{
    public class TableFileWriter
    {
        private readonly string _fileName;
        private uint _crc;

        private readonly List<long> _blockOffsets = new List<long>();
        private readonly Filter _currentFilter = new Filter();

        public TableFileWriter(string fileName)
        {
            _fileName = fileName;
        }

        public string FileName => _fileName;

        public async Task SaveMemoryTable(MemoryTable skipList)
        {
            using var fs = System.IO.File.OpenWrite(_fileName);
            var pipeWriter = PipeWriter.Create(fs);

            var iterator = skipList.GetEnumerator();
            if (!iterator.MoveNext())
            {
                throw new InvalidOperationException("The skiplist didn't have any nodes!");
            }

            var firstKeyInFile = iterator.Current.Key.ToArray();
            var itemCount = 0;

            while (!WriteBlock(pipeWriter, iterator, ref itemCount))
            {
                await pipeWriter.FlushAsync();
            }

            var lastKeyInFile = iterator.Current.Key.ToArray();

            WriteTOC(pipeWriter, firstKeyInFile, lastKeyInFile, itemCount);

            await pipeWriter.FlushAsync();

            await pipeWriter.CompleteAsync();

        }

        private void WriteTOC(PipeWriter pipeWriter, ReadOnlySpan<byte> firstKey, ReadOnlySpan<byte> lastKey, int count)
        {
            var currentLocation = _blockOffsets.Count * FileConsts.PageSize;

            var filterOffset = currentLocation;
            var filterSize = _currentFilter.WriteToPipe(pipeWriter);

            currentLocation += filterSize;

            var statsOffset = currentLocation;
            var statsSize = WriteStats(pipeWriter, firstKey, lastKey, count);
            currentLocation += statsSize;

            var blockOffsetsOffset = currentLocation;
            var blockOffsetsSize = WriteBlockOffsets(pipeWriter);

            var tocSize = 3 * Unsafe.SizeOf<TocEntry>();
            tocSize += sizeof(uint) + sizeof(int) + sizeof(int);

            var span = pipeWriter.GetSpan(tocSize);
            span = span[..tocSize];
            var totalSpan = span;

            span = WriteTOCEntry(span, TocEntryType.Filter, filterOffset, filterSize);
            span = WriteTOCEntry(span, TocEntryType.BlockOffsets, blockOffsetsOffset, blockOffsetsSize);
            span = WriteTOCEntry(span, TocEntryType.Statistics, statsOffset, statsSize);

            BinaryPrimitives.WriteInt32LittleEndian(span, FileConsts.Version);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteInt32LittleEndian(span, tocSize);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteUInt32LittleEndian(span, FileConsts.MagicNumber);
            span = span[sizeof(uint)..];
            _crc = CalculateCRC(_crc, totalSpan);
            pipeWriter.Advance(tocSize);
        }

        private Span<byte> WriteTOCEntry(Span<byte> span, TocEntryType tocType, long offset, int length)
        {
            var tocEntry = new TocEntry() { EntryType = tocType, Offset = offset, Length = length };
            Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(span), tocEntry);
            return span[Unsafe.SizeOf<TocEntry>()..];
        }

        private int WriteBlockOffsets(PipeWriter pipeWriter)
        {
            var sizeToWrite = _blockOffsets.Count * sizeof(long) + sizeof(int);

            var span = pipeWriter.GetSpan(sizeToWrite);
            var totalSpan = span[..sizeToWrite];

            BinaryPrimitives.WriteInt32LittleEndian(span, _blockOffsets.Count);
            span = span[sizeof(int)..];

            foreach (var offset in _blockOffsets)
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, offset);
                span = span[sizeof(long)..];
            }

            _crc = CalculateCRC(_crc, totalSpan);
            pipeWriter.Advance(sizeToWrite);
            return sizeToWrite;
        }

        private int WriteStats(PipeWriter pipeWriter, ReadOnlySpan<byte> firstKey, ReadOnlySpan<byte> lastKey, int count)
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

            _crc = CalculateCRC(_crc, totalSpan);
            pipeWriter.Advance(totalWritten);

            return totalWritten;
        }

        private bool WriteBlock(PipeWriter pipeWriter, IEnumerator<IMemoryItem> iterator, ref int counter)
        {
            var span = pipeWriter.GetSpan(FileConsts.PageSize);
            span = span[..FileConsts.PageSize];
            var totalSpan = span;
            var currentOffset = _blockOffsets.Count * FileConsts.PageSize;
            _blockOffsets.Add(currentOffset);

            do
            {
                counter++;
                var key = iterator.Current.Key;
                var value = iterator.Current.Value;
                var sizeNeeded = (long)(sizeof(long) + sizeof(int) + key.Length + value.Length);

                if (sizeNeeded > span.Length)
                {
                    span.Fill(0);
                    _crc = CalculateCRC(_crc, totalSpan);
                    pipeWriter.Advance(FileConsts.PageSize);
                    return false;
                }

                _currentFilter.AddKey(key);

                BinaryPrimitives.WriteInt64LittleEndian(span, sizeNeeded);
                span = span[sizeof(long)..];
                BinaryPrimitives.WriteInt32LittleEndian(span, key.Length);
                span = span[sizeof(int)..];
                key.CopyTo(span);
                span = span[key.Length..];
                value.CopyTo(span);
                span = span[value.Length..];

            } while (iterator.MoveNext());

            span.Fill(0);
            _crc = CalculateCRC(_crc, totalSpan);
            pipeWriter.Advance(FileConsts.PageSize);

            return true;
        }

        // TODO: Complete CRC Check
        private uint CalculateCRC(uint crc, ReadOnlySpan<byte> span)
        {
            ref var mem = ref MemoryMarshal.GetReference(span);
            var remaining = span.Length;

            while (remaining >= 4)
            {
                crc = Sse42.Crc32(crc, Unsafe.As<byte, uint>(ref mem));
                mem = ref Unsafe.Add(ref mem, sizeof(uint));
                remaining -= sizeof(uint);
            }

            if (remaining >= 2)
            {
                crc = Sse42.Crc32(crc, Unsafe.As<byte, ushort>(ref mem));
                mem = ref Unsafe.Add(ref mem, sizeof(ushort));
                remaining -= sizeof(ushort);
            }

            if (remaining == 1)
            {
                crc = Sse42.Crc32(crc, mem);
            }
            return crc;
        }
    }
}
