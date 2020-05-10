using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
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
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Filters;
using TrimDB.Core.Storage.MetaData;

namespace TrimDB.Core.Storage
{
    public class TableFileWriter
    {
        private readonly string _fileName;
        private uint _crc;

        private readonly List<long> _blockOffsets = new List<long>();
        private readonly Filter _currentFilter = new XorFilter();
        private ReadOnlyMemory<byte> _lastKey;
        private ReadOnlyMemory<byte> _firstKey;

        public TableFileWriter(string fileName)
        {
            _fileName = fileName;
        }

        public string FileName => _fileName;

        public async Task SaveMemoryTable(MemoryTable skipList)
        {
            using var fs = System.IO.File.Open(_fileName, System.IO.FileMode.CreateNew);
            var pipeWriter = PipeWriter.Create(fs);

            var iterator = skipList.GetEnumerator();


            var itemCount = 0;

            WriteBlocks(pipeWriter, iterator, ref itemCount);
            await pipeWriter.FlushAsync();

            WriteTOC(pipeWriter, _firstKey.Span, _lastKey.Span, itemCount);

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

            TableFileFooter.WriteTOC(pipeWriter, new TableOfContentsEntry() { EntryType = TableOfContentsEntryType.Filter, Length = filterSize, Offset = filterOffset },
                new TableOfContentsEntry() { EntryType = TableOfContentsEntryType.Statistics, Length = statsSize, Offset = statsOffset },
                new TableOfContentsEntry() { EntryType = TableOfContentsEntryType.BlockOffsets, Length = blockOffsetsSize, Offset = blockOffsetsOffset });
        }

        private void WriteBlocks(PipeWriter pipeWriter, IEnumerator<IMemoryItem> iterator, ref int counter)
        {
            using var blockWriter = new BlockWriter(iterator, _currentFilter);

            while (blockWriter.MoreToWrite)
            {
                var span = pipeWriter.GetSpan(FileConsts.PageSize);
                span = span[..FileConsts.PageSize];
                blockWriter.WriteBlock(span);
                pipeWriter.Advance(FileConsts.PageSize);
                _blockOffsets.Add(_blockOffsets.Count * FileConsts.PageSize);

            }
            counter = blockWriter.Count;
            _firstKey = blockWriter.FirstKey;
            _lastKey = blockWriter.LastKey;
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
