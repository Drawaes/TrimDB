using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage;

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

        public TrimDatabase(Func<MemoryTable> inMemoryFunc, int levels, string databaseFolder)
        {
            if (!System.IO.Directory.Exists(databaseFolder))
            {
                System.IO.Directory.CreateDirectory(databaseFolder);
            }

            _databaseFolder = databaseFolder;
            _inMemoryFunc = inMemoryFunc;

            var unsorted = new UnsortedStorageLayer(1, _databaseFolder);
            _ioScheduler = new IOScheduler(1, unsorted);
            _storageLayers.Add(unsorted);
            for (var i = 2; i <= levels; i++)
            {
                _storageLayers.Add(new SortedStorageLayer(i, _databaseFolder));
            }
            _skipList = _inMemoryFunc();
        }

        public ValueTask<Memory<byte>> GetAsync(ReadOnlySpan<byte> key)
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
                return new ValueTask<Memory<byte>>(memory);
            }

            var oldLists = _oldInMemoryTables;
            foreach (var oldsl in oldLists)
            {
                result = oldsl.TryGet(key, out value);
                if (result == SearchResult.Deleted)
                {
                    return new ValueTask<Memory<byte>>(new Memory<byte>());
                }
                else if (result == SearchResult.Found)
                {
                    var memory = value.ToArray();
                    return new ValueTask<Memory<byte>>(memory);
                }
            }

            var copiedMemory = MemoryPool<byte>.Shared.Rent(key.Length);
            key.CopyTo(copiedMemory.Memory.Span);
            return GetAsyncInternal(copiedMemory.Memory);
        }

        public async ValueTask<Memory<byte>> GetAsyncInternal(Memory<byte> key)
        {
            var keyHash = _hasher.ComputeHash64(key.Span);

            foreach (var storage in _storageLayers)
            {
                var (result, value) = await storage.GetAsync(key, keyHash).ConfigureAwait(false);
                if (result == SearchResult.Deleted || result == SearchResult.Found)
                {
                    return value;
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

        // TODO : Solve the overwriting the old skip list if its not on disk yet
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

