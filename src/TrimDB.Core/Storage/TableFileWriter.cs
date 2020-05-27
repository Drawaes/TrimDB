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

        private TableMetaData _metaData;

        public TableFileWriter(string fileName)
        {
            _metaData = new TableMetaData();
            _fileName = fileName;
        }

        public string FileName => _fileName;

        public async Task SaveMemoryTable(MemoryTable skipList)
        {
            using var fs = System.IO.File.Open(_fileName, System.IO.FileMode.CreateNew);
            var pipeWriter = PipeWriter.Create(fs);

            var iterator = skipList.GetEnumerator();
            if (!iterator.MoveNext()) throw new ArgumentOutOfRangeException("Empty iterator nothing to save");
            

            var itemCount = 0;

            WriteBlocks(pipeWriter, iterator, ref itemCount);
            await pipeWriter.FlushAsync();

            WriteTOC(pipeWriter, _metaData.FirstKey.Span, _metaData.LastKey.Span, itemCount);
            await pipeWriter.FlushAsync();

            await pipeWriter.CompleteAsync();
        }

        private void WriteTOC(PipeWriter pipeWriter, ReadOnlySpan<byte> firstKey, ReadOnlySpan<byte> lastKey, int count)
        {
            var currentLocation = _metaData.BlockCount * FileConsts.PageSize;

            var filterSize = _metaData.Filter.WriteToPipe(pipeWriter);
            _metaData.AddTableEntry(currentLocation, filterSize, TableOfContentsEntryType.Filter);
            currentLocation += filterSize;

            _metaData.Count = count;
            var statsSize = _metaData.WriteStats(pipeWriter);
            _metaData.AddTableEntry(currentLocation, statsSize, TableOfContentsEntryType.Statistics);
            currentLocation += statsSize;

            var blockOffsetsSize = _metaData.WriteBlockOffsets(pipeWriter);
            _metaData.AddTableEntry(currentLocation, blockOffsetsSize, TableOfContentsEntryType.BlockOffsets);

            _metaData.WriteTOC(pipeWriter);
        }

        private void WriteBlocks(PipeWriter pipeWriter, IEnumerator<IMemoryItem> iterator, ref int counter)
        {
            using var blockWriter = new BlockWriter(iterator, _metaData.Filter);
            
            while (blockWriter.MoreToWrite)
            {
                var span = pipeWriter.GetSpan(FileConsts.PageSize);
                span = span[..FileConsts.PageSize];
                _metaData.AddBlockOffset(_metaData.BlockCount * FileConsts.PageSize, iterator.Current.Key.ToArray());
                blockWriter.WriteBlock(span);
                pipeWriter.Advance(FileConsts.PageSize);
                

            }
            counter = blockWriter.Count;
            _metaData.FirstKey = blockWriter.FirstKey;
            _metaData.LastKey = blockWriter.LastKey;
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
