using System;
using System.Buffers;
using System.Collections.Generic;
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
    public class TrimDatabase
    {
        private MemoryTable _skipList;
        private readonly Func<MemoryTable> _inMemoryFunc;
        private List<MemoryTable> _oldInMemoryTables = new List<MemoryTable>();
        private readonly List<StorageLayer> _storageLayers = new List<StorageLayer>();
        private readonly IHashFunction _hasher = new MurmurHash3();
        private readonly SemaphoreSlim _skipListLock = new SemaphoreSlim(1);
        private readonly string _databaseFolder;
        private readonly IOScheduler _ioScheduler;
        private readonly BlockCache _blockCache;

        public TrimDatabase(Func<MemoryTable> inMemoryFunc, BlockCache blockCache, int levels, string databaseFolder)
        {
            _blockCache = blockCache;
            if (!System.IO.Directory.Exists(databaseFolder))
            {
                System.IO.Directory.CreateDirectory(databaseFolder);
            }

            _databaseFolder = databaseFolder;
            _inMemoryFunc = inMemoryFunc;

            var unsorted = new UnsortedStorageLayer(1, _databaseFolder, _blockCache);
            _ioScheduler = new IOScheduler(1, unsorted, this);
            _storageLayers.Add(unsorted);
            for (var i = 2; i <= levels; i++)
            {
                _storageLayers.Add(new SortedStorageLayer(i, _databaseFolder, _blockCache));
            }
            _skipList = _inMemoryFunc();
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
                    Interlocked.Exchange(ref _skipList, _inMemoryFunc());
                    await _ioScheduler.ScheduleSave(sl);
                }
            }
            finally
            {
                _skipListLock.Release();
            }
        }
    }
}

