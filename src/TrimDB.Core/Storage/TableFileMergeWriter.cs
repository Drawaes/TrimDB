using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Filters;

namespace TrimDB.Core.Storage
{
    public class TableFileMergeWriter
    {
        private StorageLayer _layer;
        private List<TableFile> _newTableFiles = new List<TableFile>();

        private string _fileName;
        private Stream _fileStream;
        private PipeWriter _filePipe;
        private ReadOnlyMemory<byte> _firstKey;
        private ReadOnlyMemory<byte> _lastKey;
        private Filter _currentFilter;
        private long _currentFileSize = 0;
        private List<long> _blockOffsets;
        private int _itemCount;

        public TableFileMergeWriter(StorageLayer layer)
        {
            _layer = layer;
        }

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

            TableFileFooter.WriteTOC(_filePipe, new TocEntry() { EntryType = TocEntryType.Filter, Length = filterSize, Offset = filterOffset },
                new TocEntry() { EntryType = TocEntryType.Statistics, Length = statsSize, Offset = statsOffset },
                new TocEntry() { EntryType = TocEntryType.BlockOffsets, Length = blockOffsetsSize, Offset = blockOffsetsOffset });

            await _filePipe.FlushAsync();
            await _filePipe.CompleteAsync();
            await _fileStream.DisposeAsync();

            var tf = new TableFile(_fileName);
            _newTableFiles.Add(tf);
            await tf.LoadAsync();
        }



        public async Task WriteFromMerger(TableFileMerger merger)
        {
            StartNewFile();
            while (await merger.MoveNextAsync())
            {
                // Set the first key as we must be at the start of a new file
                if (_firstKey.Length == 0)
                {
                    _firstKey = merger.Current.Key.ToArray();
                }



                if (_currentFileSize >= _layer.MaxFileSize)
                {
                    _lastKey = merger.Current.Key.ToArray();

                    await CloseOffCurrentTable();
                }
            }
        }
    }
}
