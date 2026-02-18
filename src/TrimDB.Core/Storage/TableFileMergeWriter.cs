using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Layers;
using TrimDB.Core.Storage.MetaData;

namespace TrimDB.Core.Storage
{
    public class TableFileMergeWriter
    {
        private readonly StorageLayer _layer;
        private readonly BlockCache _blockCache;
        private readonly List<TableFile> _newTableFiles = new List<TableFile>();

        private string _fileName = null!;
        private Stream _fileStream = null!;
        private PipeWriter _filePipe = null!;
        private long _currentFileSize = 0;
        private TableMetaData _metaData = new TableMetaData(0, true);
        private int _lastTableKeyCount = 0;
        private bool _loadNewFiles;

        // Reusable list for collecting items per block (avoids per-block list allocation)
        private readonly List<BlockItem> _blockItems = new List<BlockItem>();

        public TableFileMergeWriter(StorageLayer layer, BlockCache blockCache, bool loadNewFiles = true)
        {
            _loadNewFiles = loadNewFiles;
            _layer = layer;
            _blockCache = blockCache;
        }

        public List<TableFile> NewTableFiles => _newTableFiles;

        private void StartNewFile()
        {
            _fileName = _layer.GetNextFileName();
            _fileStream = File.OpenWrite(_fileName);
            _filePipe = PipeWriter.Create(_fileStream);
            _currentFileSize = 0;
            _metaData = new TableMetaData(_lastTableKeyCount, true);
        }

        private async Task CloseOffCurrentTable()
        {
            var currentLocation = (long)_metaData.BlockCount * FileConsts.PageSize;

            var filterSize = _metaData.Filter.WriteToPipe(_filePipe);
            _metaData.AddTableEntry(currentLocation, filterSize, TableOfContentsEntryType.Filter);
            currentLocation += filterSize;

            var statsSize = _metaData.WriteStats(_filePipe);
            _metaData.AddTableEntry(currentLocation, statsSize, TableOfContentsEntryType.Statistics);
            currentLocation += statsSize;

            var blockOffsetsSize = _metaData.WriteBlockOffsets(_filePipe);
            _metaData.AddTableEntry(currentLocation, blockOffsetsSize, TableOfContentsEntryType.BlockOffsets);
            currentLocation += blockOffsetsSize;

            var crcSize = _metaData.WriteBlockCRCs(_filePipe);
            _metaData.AddTableEntry(currentLocation, crcSize, TableOfContentsEntryType.BlockCRCs);

            _metaData.WriteTOC(_filePipe);

            await _filePipe.FlushAsync();
            await _filePipe.CompleteAsync();
            await _fileStream.DisposeAsync();

            if (_loadNewFiles)
            {
                var tf = new TableFile(_fileName, _blockCache);
                _newTableFiles.Add(tf);
                await tf.LoadAsync();
            }
            _lastTableKeyCount = _metaData.Count;
        }

        public async Task WriteFromMerger(TableFileMerger merger)
        {
            await merger.MoveNextAsync();
            while (true)
            {
                if (_metaData.FirstKey.Length == 0)
                {
                    StartNewFile();
                    _metaData.FirstKey = merger.Current.Key.ToArray();
                }

                var mergerFinished = await WriteBlockAsync(merger);
                await _filePipe.FlushAsync();

                if (_currentFileSize >= _layer.MaxFileSize || mergerFinished)
                {
                    await CloseOffCurrentTable();
                    _metaData.FirstKey = Array.Empty<byte>();
                    if (mergerFinished) return;
                }
            }
        }

        private async Task<bool> WriteBlockAsync(TableFileMerger merger)
        {
            _blockItems.Clear();

            var currentOffset = (long)_metaData.BlockCount * FileConsts.PageSize;
            _metaData.AddBlockOffset(currentOffset, merger.Current.Key.ToArray());

            // Phase 1: Collect items (async) — track space to know when block is full
            var availableSpace = FileConsts.PageSize - SlottedBlock.HeaderSize;

            do
            {
                var key = merger.Current.Key;
                var value = merger.Current.Value;
                var isDeleted = merger.Current.IsDeleted;

                var needed = SlottedBlockBuilder.SpaceNeeded(key.Length, value.Length, isDeleted);
                if (needed > availableSpace && _blockItems.Count > 0)
                {
                    // Block full — write what we have, merger still positioned at current item
                    FlushCollectedBlock();
                    return false;
                }

                _blockItems.Add(new BlockItem(key.ToArray(), isDeleted ? Array.Empty<byte>() : value.ToArray(), isDeleted));
                availableSpace -= needed;

                _metaData.Count++;
                _metaData.Filter.AddKey(key);
                _metaData.LastKey = _blockItems[^1].Key;

            } while (await merger.MoveNextAsync());

            // Merger exhausted
            FlushCollectedBlock();
            return true;
        }

        /// <summary>
        /// Phase 2: Build the slotted block synchronously from collected items.
        /// The ref struct SlottedBlockBuilder lives entirely in this sync scope.
        /// </summary>
        private void FlushCollectedBlock()
        {
            var blockSpan = _filePipe.GetSpan(FileConsts.PageSize)[..FileConsts.PageSize];
            var builder = new SlottedBlockBuilder(blockSpan);

            foreach (var item in _blockItems)
            {
                builder.TryAdd(item.Key, item.Value, item.IsDeleted);
            }

            builder.Finish();

            var blockIndex = _metaData.BlockCount - 1;
            var crc = Crc32Helper.Compute(blockSpan[..FileConsts.PageSize]);
            _metaData.SetBlockCRC(blockIndex, crc);

            _filePipe.Advance(FileConsts.PageSize);
            _currentFileSize += FileConsts.PageSize;
        }

        private readonly struct BlockItem
        {
            public readonly byte[] Key;
            public readonly byte[] Value;
            public readonly bool IsDeleted;

            public BlockItem(byte[] key, byte[] value, bool isDeleted)
            {
                Key = key;
                Value = value;
                IsDeleted = isDeleted;
            }
        }
    }
}
