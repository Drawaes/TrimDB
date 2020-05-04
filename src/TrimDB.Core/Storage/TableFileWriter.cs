using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Drawing;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.CompilerServices;
using TrimDB.Core.SkipList;
using TrimDB.Core.Storage.Filters;

namespace TrimDB.Core.Storage
{
    public class TableFileWriter
    {
        private int _level;
        private int _fileId;
        private string _fileName;
        private const int PageSize = 4096;
        private const uint MagicNumber = 0xDEADBEAF;
        private const int Version = 1;

        private List<long> _blockOffsets = new List<long>();
        private Filter _currentFilter = new Filter();

        public TableFileWriter(string pathToFiles, int level, int fileId)
        {
            _level = level;
            _fileId = fileId;
            _fileName = System.IO.Path.Combine(pathToFiles, $"Level{level}_{fileId}.trim");
        }

        public async Task SaveSkipList(SkipList.SkipList skipList)
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

            await pipeWriter.CompleteAsync();

        }

        private void WriteTOC(PipeWriter pipeWriter, ReadOnlySpan<byte> firstKey, ReadOnlySpan<byte> lastKey, int count)
        {
            var currentLocation = _blockOffsets.Count * PageSize;

            var filterOffset = currentLocation;
            var filterSize = _currentFilter.WriteToPipe(pipeWriter);

            currentLocation += filterSize;

            var statsOffset = currentLocation;
            var statsSize = WriteStats(pipeWriter, firstKey, lastKey, count);
            currentLocation += statsSize;


            var blockOffsetsOffset = currentLocation;
            var blockOffsetsSize = WriteBlockOffsets(pipeWriter);

            var tocSize = 3 * (sizeof(long) + sizeof(int) + sizeof(short));
            tocSize += sizeof(uint) + sizeof(int) + sizeof(int);

            var span = pipeWriter.GetSpan(tocSize);

            span = WriteTOCEntry(span, TOCType.Filter, filterOffset, filterSize);
            span = WriteTOCEntry(span, TOCType.BlockOffsets, blockOffsetsOffset, blockOffsetsSize);
            span = WriteTOCEntry(span, TOCType.Statistics, statsOffset, statsSize);

            BinaryPrimitives.WriteInt32LittleEndian(span, Version);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteInt32LittleEndian(span, tocSize);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteUInt32LittleEndian(span, MagicNumber);
            span = span[sizeof(uint)..];

            pipeWriter.Advance(tocSize);
        }

        private Span<byte> WriteTOCEntry(Span<byte> span, TOCType tocType, long offset, int length)
        {
            BinaryPrimitives.WriteInt16LittleEndian(span, (short) tocType);
            span = span[sizeof(short)..];
            BinaryPrimitives.WriteInt64LittleEndian(span, offset);
            span = span[sizeof(long)..];
            BinaryPrimitives.WriteInt32LittleEndian(span, length);
            return span[sizeof(int)..];
        }

        private int WriteBlockOffsets(PipeWriter pipeWriter)
        {
            var sizeToWrite = _blockOffsets.Count * sizeof(long) + sizeof(int);

            var span = pipeWriter.GetSpan(sizeToWrite);
            BinaryPrimitives.WriteInt32LittleEndian(span, _blockOffsets.Count);
            span = span[sizeof(int)..];

            foreach (var offset in _blockOffsets)
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, offset);
                span = span[sizeof(long)..];
            }

            pipeWriter.Advance(sizeToWrite);
            return sizeToWrite;
        }

        private int WriteStats(PipeWriter pipeWriter, ReadOnlySpan<byte> firstKey, ReadOnlySpan<byte> lastKey, int count)
        {
            var totalWritten = firstKey.Length + sizeof(int) * 3 + lastKey.Length;

            var memory = pipeWriter.GetSpan(totalWritten);
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

        private bool WriteBlock(PipeWriter pipeWriter, IEnumerator<SkipListItem> iterator, ref int counter)
        {
            var span = pipeWriter.GetSpan(PageSize);
            span = span[..PageSize];
            var currentOffset = _blockOffsets.Count * PageSize;
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
                    pipeWriter.Advance(PageSize);
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
            pipeWriter.Advance(PageSize);

            return true;
        }

        // TODO: Complete CRC Check
        private void CalculateCRC(ReadOnlySpan<byte> span)
        {
            ref var mem = ref MemoryMarshal.GetReference(span);
        }

        private enum TOCType : short
        {
            BlockOffsets,
            Statistics,
            Filter
        }
    }
}
