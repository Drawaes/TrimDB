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
        private readonly Filter _currentFilter = new XorFilter();

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
            var statsSize = TableFileFooter.WriteStats(pipeWriter, firstKey, lastKey, count);
            currentLocation += statsSize;

            var blockOffsetsOffset = currentLocation;
            var blockOffsetsSize = TableFileFooter.WriteBlockOffsets(pipeWriter, _blockOffsets);

            TableFileFooter.WriteTOC(pipeWriter, new TocEntry() { EntryType = TocEntryType.Filter, Length = filterSize, Offset = filterOffset },
                new TocEntry() { EntryType = TocEntryType.Statistics, Length = statsSize, Offset = statsOffset },
                new TocEntry() { EntryType = TocEntryType.BlockOffsets, Length = blockOffsetsSize, Offset = blockOffsetsOffset });
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
