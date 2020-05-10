using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Filters;
using TrimDB.Core.Storage.Layers;
using TrimDB.Core.Storage.MetaData;

namespace TrimDB.Core.Storage
{
    public class TableFileMergeWriter
    {
        private readonly StorageLayer _layer;
        private readonly BlockCache _blockCache;
        private readonly List<TableFile> _newTableFiles = new List<TableFile>();

        private string _fileName;
        private Stream _fileStream;
        private PipeWriter _filePipe;
        private ReadOnlyMemory<byte> _firstKey;
        private ReadOnlyMemory<byte> _lastKey;
        private Filter _currentFilter;
        private long _currentFileSize = 0;
        private List<long> _blockOffsets;
        private int _itemCount;


        public TableFileMergeWriter(StorageLayer layer, BlockCache blockCache)
        {
            _layer = layer;
            _blockCache = blockCache;
        }

        public List<TableFile> NewTableFiles => _newTableFiles;

        private void StartNewFile()
        {
            _fileName = _layer.GetNextFileName();
            _fileStream = File.OpenWrite(_fileName);
            _filePipe = PipeWriter.Create(_fileStream);
            _currentFilter = new XorFilter();
            _currentFileSize = 0;
            _blockOffsets = new List<long>();
            _firstKey = default;
            _itemCount = 0;
        }

        private async Task CloseOffCurrentTable()
        {
            var currentLocation = _blockOffsets.Count * FileConsts.PageSize;

            var filterOffset = currentLocation;
            var filterSize = _currentFilter.WriteToPipe(_filePipe);

            currentLocation += filterSize;

            var statsOffset = currentLocation;
            var statsSize = TableFileFooter.WriteStats(_filePipe, _firstKey.Span, _lastKey.Span, _itemCount);
            currentLocation += statsSize;

            var blockOffsetsOffset = currentLocation;
            var blockOffsetsSize = TableFileFooter.WriteBlockOffsets(_filePipe, _blockOffsets);

            TableFileFooter.WriteTOC(_filePipe, new TableOfContentsEntry() { EntryType = TableOfContentsEntryType.Filter, Length = filterSize, Offset = filterOffset },
                new TableOfContentsEntry() { EntryType = TableOfContentsEntryType.Statistics, Length = statsSize, Offset = statsOffset },
                new TableOfContentsEntry() { EntryType = TableOfContentsEntryType.BlockOffsets, Length = blockOffsetsSize, Offset = blockOffsetsOffset });

            await _filePipe.FlushAsync();
            await _filePipe.CompleteAsync();
            await _fileStream.DisposeAsync();

            var tf = new TableFile(_fileName, _blockCache);
            _newTableFiles.Add(tf);
            await tf.LoadAsync();
        }

        public async Task WriteFromMerger(TableFileMerger merger)
        {
            await merger.MoveNextAsync();
            while (true)
            {
                // Set the first key as we must be at the start of a new file
                if (_firstKey.Length == 0)
                {
                    StartNewFile();
                    _firstKey = merger.Current.Key.ToArray();
                }

                var mergerFinished = await WriteBlock(merger);
                await _filePipe.FlushAsync();

                if (_currentFileSize >= _layer.MaxFileSize || mergerFinished)
                {
                    _lastKey = merger.Current.Key.ToArray();

                    await CloseOffCurrentTable();
                    if (mergerFinished) return;
                }
            }
        }

        private async Task<bool> WriteBlock(TableFileMerger merger)
        {
            var memBlock = _filePipe.GetMemory(FileConsts.PageSize);
            var fullMemBlock = memBlock;

            var currentOffset = _blockOffsets.Count * FileConsts.PageSize;
            _blockOffsets.Add(currentOffset);

            do
            {
                _itemCount++;

                var sizeNeeded = SizeNeededOnBlock(merger.Current);

                if (sizeNeeded > memBlock.Length)
                {
                    memBlock.Span.Fill(0);
                    _filePipe.Advance(FileConsts.PageSize);
                    _currentFileSize += FileConsts.PageSize;
                    return false;
                }

                _currentFilter.AddKey(merger.Current.Key);
                BinaryPrimitives.WriteInt64LittleEndian(memBlock.Span, sizeNeeded);
                memBlock = memBlock[sizeof(long)..];

                var keyLength = merger.Current.Key.Length;

                BinaryPrimitives.WriteInt32LittleEndian(memBlock.Span, keyLength);
                memBlock = memBlock[sizeof(int)..];

                merger.Current.Key.CopyTo(memBlock.Span);
                memBlock = memBlock[keyLength..];
                merger.Current.Value.CopyTo(memBlock.Span);
                memBlock = memBlock[merger.Current.Value.Length..];

            } while (await merger.MoveNextAsync());

            memBlock.Span.Fill(0);
            _filePipe.Advance(FileConsts.PageSize);
            _currentFileSize += FileConsts.PageSize;
            return true;
        }

        private long SizeNeededOnBlock(IMemoryItem memoryItem)
        {
            var sizeNeeded = (long)(sizeof(long) + sizeof(int) + memoryItem.Key.Length + memoryItem.Value.Length);
            return sizeNeeded;
        }
    }
}
