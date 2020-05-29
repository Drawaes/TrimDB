using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Layers;

namespace TrimDB.Core
{
    public class TrimDatabase : IAsyncDisposable
    {
        private MemoryTable _skipList;
        private List<MemoryTable> _oldInMemoryTables = new List<MemoryTable>();
        private readonly List<StorageLayer> _storageLayers = new List<StorageLayer>();
        private readonly IHashFunction _hasher = new MurmurHash3();
        private readonly SemaphoreSlim _skipListLock = new SemaphoreSlim(1);
        private readonly string _databaseFolder;
        private IOScheduler _ioScheduler;
        private readonly BlockCache _blockCache;
        private TrimDatabaseOptions _options;

        public TrimDatabase(TrimDatabaseOptions options)
        {
            _blockCache = options.BlockCache();
            _options = options;
            if (!System.IO.Directory.Exists(options.DatabaseFolder))
            {
                System.IO.Directory.CreateDirectory(options.DatabaseFolder);
            }

            _databaseFolder = options.DatabaseFolder;

            var unsorted = new UnsortedStorageLayer(1, _databaseFolder, _blockCache);

            _storageLayers.Add(unsorted);

            for (var i = 2; i <= _options.Levels; i++)
            {
                var filesAtLevel = (int)Math.Ceiling(_options.FirstLevelMaxFileCount * Math.Pow(_options.FirstLevelMaxFileCount, i - 2));
                _storageLayers.Add(new SortedStorageLayer(i, _databaseFolder, _blockCache, _options.FileSize, filesAtLevel));
            }
            if (!_options.OpenReadOnly)
            {
                _skipList = _options.MemoryTable();
            }
        }

        public async Task LoadAsync()
        {
            foreach (var sl in _storageLayers)
            {
                await sl.LoadLayer().ConfigureAwait(false);
            }
            if (!_options.OpenReadOnly)
            {
                _ioScheduler = new IOScheduler(1, (UnsortedStorageLayer)_storageLayers[0], this);
            }
        }

        internal List<StorageLayer> StorageLayers => _storageLayers;

        public BlockCache BlockCache => _blockCache;

        public ValueTask<ReadOnlyMemory<byte>> GetAsync(ReadOnlySpan<byte> key)
        {
            var sl = _skipList;
            var result = sl.TryGet(key, out var value);

            if (result == SearchResult.Deleted)
            {
                return default;
            }
            else if (result == SearchResult.Found)
            {
                var memory = value.ToArray();
                return new ValueTask<ReadOnlyMemory<byte>>(memory);
            }

            var oldLists = _oldInMemoryTables;
            foreach (var oldsl in oldLists)
            {
                result = oldsl.TryGet(key, out value);
                if (result == SearchResult.Deleted)
                {
                    return new ValueTask<ReadOnlyMemory<byte>>(new ReadOnlyMemory<byte>());
                }
                else if (result == SearchResult.Found)
                {
                    var memory = value.ToArray();
                    return new ValueTask<ReadOnlyMemory<byte>>(memory);
                }
            }

            var copiedMemory = MemoryPool<byte>.Shared.Rent(key.Length);
            key.CopyTo(copiedMemory.Memory.Span);
            return GetAsyncInternal(copiedMemory.Memory.Slice(0, key.Length));
        }

        internal void RemoveMemoryTable(MemoryTable sl)
        {
            while (true)
            {
                var memTable = _oldInMemoryTables;
                var list = new List<MemoryTable>(_oldInMemoryTables);
                list.Remove(sl);
                if (Interlocked.CompareExchange(ref _oldInMemoryTables, list, memTable) == memTable) return;
            }
        }

        public async ValueTask<ReadOnlyMemory<byte>> GetAsyncInternal(ReadOnlyMemory<byte> key)
        {
            var keyHash = _hasher.ComputeHash64(key.Span);

            foreach (var storage in _storageLayers)
            {
                var result = await storage.GetAsync(key, keyHash).ConfigureAwait(false);
                if (result.Result == SearchResult.Deleted || result.Result == SearchResult.Found)
                {
                    return result.Value;
                }
            }
            return default;
        }

        internal async ValueTask<SearchResult> DoesKeyExistBelowLevel(ReadOnlyMemory<byte> key, int levelId)
        {
            var keyHash = _hasher.ComputeHash64(key.Span);
            for (var i = levelId; i < _storageLayers.Count; i++)
            {
                var storage = _storageLayers[i];
                var result = await storage.GetAsync(key, keyHash);
                if (result.Result == SearchResult.Deleted || result.Result == SearchResult.Found)
                {
                    return result.Result;
                }
            }
            return SearchResult.NotFound;
        }

        public ValueTask<bool> DeleteAsync(ReadOnlySpan<byte> key)
        {
            throw new NotImplementedException();
        }

        public async Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
        {
            while (true)
            {
                var sl = Volatile.Read(ref _skipList);

                if (!sl.Put(key.Span, value.Span))
                {
                    await SwitchInMemoryTable(sl);
                    continue;
                }
                return;
            }
        }

        private async Task SwitchInMemoryTable(MemoryTable sl)
        {
            await _skipListLock.WaitAsync();
            try
            {
                if (Volatile.Read(ref _skipList) == sl)
                {
                    var list = new List<MemoryTable>(_oldInMemoryTables) { sl };
                    Interlocked.Exchange(ref _oldInMemoryTables, list);
                    Interlocked.Exchange(ref _skipList, _options.MemoryTable());
                    await _ioScheduler.ScheduleSave(sl);
                }
            }
            finally
            {
                _skipListLock.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_ioScheduler == null) return;
            // Set the current in memory database to null
            await _skipListLock.WaitAsync();
            try
            {
                MemoryTable old = null;
                old = Interlocked.Exchange(ref _skipList, old);
                await _ioScheduler.ScheduleSave(old);
            }
            finally
            {
                _skipListLock.Release();
            }
            await _ioScheduler.DisposeAsync();
        }
    }
}

