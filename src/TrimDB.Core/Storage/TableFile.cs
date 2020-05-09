using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage.BlockCache;
using TrimDB.Core.Storage.Filters;

namespace TrimDB.Core.Storage
{
    public class TableFile : IAsyncEnumerable<IMemoryItem>
    {
        private readonly string _fileName;
        private ReadOnlyMemory<byte> _toc;
        private ReadOnlyMemory<byte> _firstKey;
        private ReadOnlyMemory<byte> _lastKey;
        private TocEntry[] _tocEntries;
        private long[] _blockEntries;
        private readonly int _index;
        private readonly int _level;
        private Filter _filter = new XorFilter();
        private CountdownEvent _countDown = new CountdownEvent(1);
        private BlockCache.BlockCache _blockCache;

        public TableFile(string filename, BlockCache.BlockCache blockCache)
        {
            _blockCache = blockCache;
            _fileName = filename;
            var numbers = Path.GetFileNameWithoutExtension(filename)["Level".Length..].Split('_');
            _level = int.Parse(numbers[0]);
            _index = int.Parse(numbers[1]);
        }

        public int Index => _index;
        public int Level => _level;
        public ReadOnlyMemory<byte> FirstKey => _firstKey;
        public ReadOnlyMemory<byte> LastKey => _lastKey;
        public int BlockCount => _blockEntries.Length;
        public string FileName => _fileName;
        public FileIdentifier FileId => new FileIdentifier() { FileId = _index, Level = _level };

        internal ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            //if (!_filter.MayContainKey((long)hash))
            //{
            //    return SearchResultValue.CreateValueTask(SearchResult.NotFound);
            //}

            var compare = key.Span.SequenceCompareTo(_firstKey.Span);
            if (compare < 0)
            {
                // Search is before the files range
                return SearchResultValue.CreateValueTask(SearchResult.NotFound);
            }
            else if (compare == 0)
            {
                return GetFirstValue();

                async ValueTask<SearchResultValue> GetFirstValue()
                {
                    using var firstBlock = await GetKVBlock(0);
                    firstBlock.TryGetNextKey(out _);
                    return new SearchResultValue() { Result = SearchResult.Found, Value = firstBlock.GetCurrentValue() };
                }
            }

            compare = key.Span.SequenceCompareTo(_lastKey.Span);
            if (compare > 0)
            {
                // Search is after the files range
                return SearchResultValue.CreateValueTask(SearchResult.NotFound);
            }
            else if (compare == 0)
            {
                return GetLastValue();
                async ValueTask<SearchResultValue> GetLastValue()
                {
                    using var firstBlock = await GetKVBlock(0);
                    firstBlock.GetLastKey();
                    return new SearchResultValue() { Result = SearchResult.Found, Value = firstBlock.GetCurrentValue() };
                }
            }

            // Do a binary search
            return BinarySearchBlocks(key);
        }

        private async ValueTask<SearchResultValue> BinarySearchBlocks(ReadOnlyMemory<byte> key)
        {
            var min = 0;
            var max = _blockEntries.Length - 1;

            while (min <= max)
            {
                var mid = (min + max) / 2;
                using var block = await GetKVBlock(mid);
                var result = block.TryFindKey(key.Span);
                if (result == BlockReader.KeySearchResult.Found)
                {
                    return new SearchResultValue() { Result = SearchResult.Found, Value = block.GetCurrentValue().ToArray() };
                }
                else if (result == BlockReader.KeySearchResult.NotFound)
                {
                    return new SearchResultValue() { Result = SearchResult.NotFound };
                }
                if (result == BlockReader.KeySearchResult.Before)
                {
                    max = mid - 1;
                }
                else
                {
                    min = mid + 1;
                }
            }

            return new SearchResultValue { Result = SearchResult.NotFound };
        }

        public IEnumerator<TableItem> GetEnumerator() => new TableItemEnumerator(this);

        internal void Dispose()
        {
            _countDown.Wait();
            _blockCache.RemoveFile(FileId);
        }

        public async Task LoadAsync()
        {
            await using (var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var length = fs.Length;
                fs.Seek(-FileConsts.PageSize, SeekOrigin.End);
                var pageBuffer = await GetBlockFromFile(fs, FileConsts.PageSize);

                await ReadFooter(fs, pageBuffer);
                ReadToc();
                await LoadStatistics(fs);
                await LoadFilter(fs);
                await LoadBlockIndex(fs);
            }
            _blockCache.RegisterFile(_fileName, FileId);
        }

        private async Task LoadBlockIndex(FileStream fs)
        {
            var blockIndex = _tocEntries.Single(te => te.EntryType == TocEntryType.BlockOffsets);

            fs.Seek(blockIndex.Offset, SeekOrigin.Begin);

            var indexBlock = await GetBlockFromFile(fs, blockIndex.Length);

            ReadBlockIndexSpan(indexBlock.Span);

            void ReadBlockIndexSpan(ReadOnlySpan<byte> data)
            {
                var expectedLength = BinaryPrimitives.ReadInt32LittleEndian(data);
                data = data.Slice(sizeof(int));
                if (data.Length / sizeof(long) != expectedLength)
                {
                    throw new IndexOutOfRangeException($"Block index was {data.Length} but expected {expectedLength * sizeof(long)}");
                }

                _blockEntries = new long[expectedLength];

                for (var i = 0; i < _blockEntries.Length; i++)
                {
                    var location = BinaryPrimitives.ReadInt64LittleEndian(data);
                    data = data.Slice(sizeof(long));
                    _blockEntries[i] = location;
                }
            }

        }

        private async Task LoadFilter(FileStream fs)
        {
            var filter = _tocEntries.Single(te => te.EntryType == TocEntryType.Filter);
            var block = await GetBlockFromFile(fs, filter.Offset, filter.Length);
            _filter.LoadFromBlock(block);
        }

        private async Task LoadStatistics(FileStream fs)
        {
            var stats = _tocEntries.Single(te => te.EntryType == TocEntryType.Statistics);
            fs.Seek(stats.Offset, SeekOrigin.Begin);
            var memoryBlock = await GetBlockFromFile(fs, stats.Length);

            LoadStats(memoryBlock.Span);

            void LoadStats(ReadOnlySpan<byte> span)
            {
                // Read first key
                var keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
                // TODO sanity check on keylength?
                var keyBuffer = new byte[keyLength];
                var tmpSpan = span.Slice(4, keyLength);
                tmpSpan.CopyTo(keyBuffer);
                _firstKey = keyBuffer;
                span = span[(sizeof(int) + keyLength)..];

                keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
                keyBuffer = new byte[keyLength];
                span.Slice(4, keyLength).CopyTo(keyBuffer);
                _lastKey = keyBuffer;
                span = span[(sizeof(int) + keyLength)..];
            }
        }

        private void ReadToc()
        {
            var span = _toc.Span;

            span = span[..^FileConsts.TocEntryOffset];
            var numEntries = span.Length / Unsafe.SizeOf<TocEntry>();
            _tocEntries = new TocEntry[numEntries];

            ref var ptr = ref MemoryMarshal.GetReference(span);

            for (var i = 0; i < _tocEntries.Length; i++)
            {
                _tocEntries[i] = Unsafe.ReadUnaligned<TocEntry>(ref ptr);
                ptr = ref Unsafe.Add(ref ptr, Unsafe.SizeOf<TocEntry>());
            }
        }

        public async Task<BlockReader> GetKVBlock(int blockId)
        {
            if (blockId >= _blockEntries.Length) return null;
            var br = new BlockReader(await _blockCache.GetBlock(FileId, blockId));
            return br;
        }

        private Task<ReadOnlyMemory<byte>> GetBlockFromFile(FileStream fs, long location, int blockSize)
        {
            fs.Seek(location, SeekOrigin.Begin);

            return GetBlockFromFile(fs, blockSize);
        }

        private async Task<ReadOnlyMemory<byte>> GetBlockFromFile(FileStream fs, int blockSize)
        {
            var pageBuffer = new byte[blockSize];

            var totalRead = 0;

            while (totalRead < blockSize)
            {
                var result = await fs.ReadAsync(pageBuffer, totalRead, pageBuffer.Length - totalRead);
                if (result == -1)
                {
                    throw new InvalidOperationException("Could not read any data from the file");
                }
                totalRead += result;
            }

            return pageBuffer;
        }

        private Task ReadFooter(FileStream fs, ReadOnlyMemory<byte> memory)
        {
            var buffer = memory.Span;
            var magicNumber = BinaryPrimitives.ReadUInt32LittleEndian(buffer[^4..]);
            if (magicNumber != FileConsts.MagicNumber)
            {
                throw new InvalidOperationException("The magic number for the file was incorrect");
            }

            var tocSize = BinaryPrimitives.ReadInt32LittleEndian(buffer[^8..]);
            var version = BinaryPrimitives.ReadInt32LittleEndian(buffer[^12..]);

            if (tocSize > FileConsts.PageSize)
            {
                return ReloadTOC(fs, tocSize);
            }
            else
            {
                _toc = memory[^tocSize..];
            }

            return Task.CompletedTask;

            async Task ReloadTOC(FileStream fs, int tocSize)
            {
                fs.Seek(-tocSize, SeekOrigin.End);
                _toc = await GetBlockFromFile(fs, tocSize);
            }
        }

        public async Task LoadToMemory()
        {
            //var memoryStream = new MemoryStream();
            //_fs.Seek(0, SeekOrigin.Begin);
            //await _fs.CopyToAsync(memoryStream);
            //var file = _fs;
            //_fs = memoryStream;
            //await file.DisposeAsync();
        }

        public IAsyncEnumerator<IMemoryItem> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            _countDown.AddCount(1);
            return new TableFileEnumerator(this);
        }

        internal void ReleaseIterator()
        {
            _countDown.Signal();
        }
    }
}
