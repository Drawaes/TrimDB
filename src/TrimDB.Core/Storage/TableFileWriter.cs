using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.MetaData;

namespace TrimDB.Core.Storage
{
    public class TableFileWriter
    {
        private readonly string _fileName;

        private TableMetaData _metaData;

        public TableFileWriter(string fileName)
        {
            _fileName = fileName;
        }

        public string FileName => _fileName;

        public async Task SaveMemoryTable(MemoryTable skipList)
        {
            _metaData = new TableMetaData(0, true);
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
            var currentLocation = (long)_metaData.BlockCount * FileConsts.PageSize;

            var filterSize = _metaData.Filter.WriteToPipe(pipeWriter);
            _metaData.AddTableEntry(currentLocation, filterSize, TableOfContentsEntryType.Filter);
            currentLocation += filterSize;

            _metaData.Count = count;
            var statsSize = _metaData.WriteStats(pipeWriter);
            _metaData.AddTableEntry(currentLocation, statsSize, TableOfContentsEntryType.Statistics);
            currentLocation += statsSize;

            var blockOffsetsSize = _metaData.WriteBlockOffsets(pipeWriter);
            _metaData.AddTableEntry(currentLocation, blockOffsetsSize, TableOfContentsEntryType.BlockOffsets);
            currentLocation += blockOffsetsSize;

            var crcSize = _metaData.WriteBlockCRCs(pipeWriter);
            _metaData.AddTableEntry(currentLocation, crcSize, TableOfContentsEntryType.BlockCRCs);

            _metaData.WriteTOC(pipeWriter);
        }

        private void WriteBlocks(PipeWriter pipeWriter, IEnumerator<IMemoryItem> iterator, ref int counter)
        {
            using var blockWriter = new BlockWriter(iterator, _metaData.Filter);

            var blockIndex = 0;
            while (blockWriter.MoreToWrite)
            {
                var span = pipeWriter.GetSpan(FileConsts.PageSize);
                span = span[..FileConsts.PageSize];
                _metaData.AddBlockOffset((long)_metaData.BlockCount * FileConsts.PageSize, iterator.Current.Key.ToArray());
                blockWriter.WriteBlock(span);

                var crc = Crc32Helper.Compute(span[..FileConsts.PageSize]);
                _metaData.SetBlockCRC(blockIndex, crc);
                blockIndex++;

                pipeWriter.Advance(FileConsts.PageSize);
            }
            counter = blockWriter.Count;
            _metaData.FirstKey = blockWriter.FirstKey;
            _metaData.LastKey = blockWriter.LastKey;
        }
    }
}
