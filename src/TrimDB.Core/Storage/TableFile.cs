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
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Filters;
using TrimDB.Core.Storage.MetaData;

namespace TrimDB.Core.Storage
{
    public class TableFile : IAsyncEnumerable<IMemoryItem>
    {
        private readonly string _fileName;
        private TableMetaData? _metaData;
        private readonly CountdownEvent _countDown = new CountdownEvent(1);
        private readonly BlockCache _blockCache;

        public TableFile(string filename, BlockCache blockCache)
        {
            _blockCache = blockCache;
            _fileName = filename;
            var numbers = Path.GetFileNameWithoutExtension(filename)["Level".Length..].Split('_');
            var level = int.Parse(numbers[0]);
            var index = int.Parse(numbers[1]);
            FileId = new FileIdentifier(level, index);
        }

        public ReadOnlyMemory<byte> FirstKey => _metaData.FirstKey;
        public ReadOnlyMemory<byte> LastKey => _metaData.LastKey;
        public int BlockCount => _metaData.BlockCount;
        public string FileName => _fileName;
        public FileIdentifier FileId { get; }

        public ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            if (!_metaData.Filter.MayContainKey((long)hash))
            {
                return SearchResultValue.CreateValueTask(SearchResult.NotFound);
            }

            var compare = key.Span.SequenceCompareTo(_metaData.FirstKey.Span);
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

            compare = key.Span.SequenceCompareTo(_metaData.LastKey.Span);
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
                    using var firstBlock = await GetKVBlock(_metaData.BlockCount-1);
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
            var max = _metaData.BlockCount - 1;

            do
            {
                var mid = (min + max) / 2;
                using var block = await GetKVBlock(mid);
                var result = block.TryFindKey(key.Span);
                if (result == BlockReader.KeySearchResult.Found)
                {
                    return new SearchResultValue() { Result = SearchResult.Found, Value = block.GetCurrentValue().ToArray() };
                }
                if (result == BlockReader.KeySearchResult.Before)
                {
                    max = mid - 1;
                }
                else if (result == BlockReader.KeySearchResult.After)
                {
                    min = mid + 1;
                }
            } while (min <= max);

            return new SearchResultValue { Result = SearchResult.NotFound };


            //else if (result == BlockReader.KeySearchResult.NotFound)
            //{
            //    return new SearchResultValue() { Result = SearchResult.NotFound };
            //}
        }

        public IEnumerator<TableItem> GetEnumerator() => new TableItemEnumerator(this);

        internal void Dispose()
        {
            _countDown.Wait();
            _blockCache.RemoveFile(FileId);
        }

        public async Task LoadAsync()
        {
            _metaData = await TableMetaData.LoadFromFileAsync(_fileName);
            _blockCache.RegisterFile(_fileName, FileId);
        }

        public async Task<BlockReader> GetKVBlock(int blockId)
        {
            if (blockId >= BlockCount) throw new IndexOutOfRangeException();
            var br = new BlockReader(await _blockCache.GetBlock(FileId, blockId));
            return br;
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
