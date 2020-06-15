using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.CachePrototype;
using TrimDB.Core.Storage.Filters;
using TrimDB.Core.Storage.Layers;
using TrimDB.Core.Storage.MetaData;

namespace TrimDB.Core.Storage
{
    public class TableFileMergeWriter2
    {
        private readonly StorageLayer _layer;
        private readonly BlockCache _blockCache;
        private readonly List<TableFile> _newTableFiles = new List<TableFile>();

        private string _fileName;
        private Stream _fileStream;
        private PipeWriter _filePipe;
        private long _currentFileSize = 0;
        private TableMetaData _metaData = new TableMetaData(0, false);
        //private TrimDatabase _database;
        private int _lastTableKeyCount = 0;
        private bool _loadNewFiles;

        public TableFileMergeWriter2(StorageLayer layer, BlockCache blockCache, bool loadNewFiles = true)
        {
            _loadNewFiles = loadNewFiles;
            //_database = database;
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
            _metaData = new TableMetaData(_lastTableKeyCount, false);
        }

        private async Task CloseOffCurrentTable()
        {
            var currentLocation = _metaData.BlockCount * FileConsts.PageSize;

            var filterSize = _metaData.Filter.WriteToPipe(_filePipe);
            _metaData.AddTableEntry(currentLocation, filterSize, TableOfContentsEntryType.Filter);
            currentLocation += filterSize;

            var statsSize = _metaData.WriteStats(_filePipe);
            _metaData.AddTableEntry(currentLocation, statsSize, TableOfContentsEntryType.Statistics);
            currentLocation += statsSize;

            var blockOffsetsSize = _metaData.WriteBlockOffsets(_filePipe);
            _metaData.AddTableEntry(currentLocation, blockOffsetsSize, TableOfContentsEntryType.BlockOffsets);

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
                // Set the first key as we must be at the start of a new file
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
            var memBlock = _filePipe.GetMemory(FileConsts.PageSize);
            var fullMemBlock = memBlock;

            var currentOffset = _metaData.BlockCount * FileConsts.PageSize;
            _metaData.AddBlockOffset(currentOffset, merger.Current.Key.ToArray());

            do
            {
                //if (merger.Current.IsDeleted)
                //{
                //    // If the key doesn't exist in a level below we do not need to store a tombstone we can just remove completely
                //    var doesExist = await _database.DoesKeyExistBelowLevel(merger.Current.Key.ToArray(), _lowestLevel);
                //    if (doesExist == SearchResult.NotFound || doesExist == SearchResult.Deleted) continue;
                //    sizeNeeded = (sizeof(int) * 2) + merger.Current.Key.Length;
                //}
                //else
                //{
               
                //}

                if (!WriteBlock(merger, ref memBlock)) return false;
            } while (await merger.MoveNextAsync());

            memBlock.Span.Fill(0);
            _filePipe.Advance(FileConsts.PageSize);
            _currentFileSize += FileConsts.PageSize;
            return true;
        }

        private bool WriteBlock(TableFileMerger merger, ref Memory<byte> memBlock)
        {
            var span = memBlock.Span;
            var remainingLength = span.Length;

            var key = merger.Current.Key;
            var value = merger.Current.Value;
            var keyLength = key.Length;
            var valueLength = value.Length;

            var sizeNeeded = (sizeof(int) * 2) + keyLength + valueLength;

            if (sizeNeeded > remainingLength)
            {
                span.Fill(0);
                _filePipe.Advance(FileConsts.PageSize);
                _currentFileSize += FileConsts.PageSize;
                return false;
            }

            _metaData.Count++;
            _metaData.Filter.AddKey(key);

            ref var currentPointer = ref MemoryMarshal.GetReference(span);
            Unsafe.WriteUnaligned(ref currentPointer, keyLength);
            currentPointer = ref Unsafe.Add(ref currentPointer, sizeof(int));

            Unsafe.CopyBlockUnaligned(ref currentPointer, ref MemoryMarshal.GetReference(key), (uint) keyLength);
            _metaData.LastKey = key.ToArray();

            currentPointer = ref Unsafe.Add(ref currentPointer, keyLength);

            //if (merger.Current.IsDeleted)
            //{
            //    BinaryPrimitives.WriteInt32LittleEndian(span, -1);
            //    span = span[sizeof(int)..];
            //    memBlock = memBlock.Slice(remainingLength - span.Length);
            //    return true;
            //}

            Unsafe.WriteUnaligned(ref currentPointer, valueLength);
            currentPointer = ref Unsafe.Add(ref currentPointer, sizeof(int));

            Unsafe.CopyBlockUnaligned(ref currentPointer, ref MemoryMarshal.GetReference(value), (uint)valueLength);

            memBlock = memBlock.Slice(sizeNeeded);
            return true;
        }
    }
}
