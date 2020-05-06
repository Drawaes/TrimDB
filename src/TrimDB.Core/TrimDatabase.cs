using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.Hashing;
using TrimDB.Core.SkipList;
using TrimDB.Core.Storage;

namespace TrimDB.Core
{
    public class TrimDatabase
    {
        private SkipList.SkipList _skipList;
        private readonly Func<SkipList.SkipList> _skipListFunc;
        private List<SkipList.SkipList> _oldSkipLists = new List<SkipList.SkipList>();
        private readonly List<StorageLayer> _storageLayers = new List<StorageLayer>();
        private readonly IHashFunction _hasher = new MurmurHash3();
        private readonly SemaphoreSlim _skipListLock = new SemaphoreSlim(1);
        private string _databaseFolder;
        private IOScheduler _ioScheduler;

        public TrimDatabase(Func<SkipList.SkipList> skipListFunc, int levels, string databaseFolder)
        {
            if (!System.IO.Directory.Exists(databaseFolder))
            {
                System.IO.Directory.CreateDirectory(databaseFolder);
            }

            _databaseFolder = databaseFolder;
            _skipListFunc = skipListFunc;

            var unsorted = new UnsortedStorageLayer(1, _databaseFolder);
            _ioScheduler = new IOScheduler(1, unsorted);
            _storageLayers.Add(unsorted);
            for (var i = 2; i <= levels; i++)
            {
                _storageLayers.Add(new SortedStorageLayer(i, _databaseFolder));
            }
            _skipList = _skipListFunc();
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

            var oldLists = _oldSkipLists;
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
                    await SwitchSkipList(sl);
                    continue;
                }
                return;
            }
        }

        // TODO : Solve the overwriting the old skip list if its not on disk yet
        private async Task SwitchSkipList(SkipList.SkipList sl)
        {
            await _skipListLock.WaitAsync();
            try
            {
                if (Volatile.Read(ref _skipList) == sl)
                {
                    var list = new List<SkipList.SkipList>(_oldSkipLists) { sl };
                    Interlocked.Exchange(ref _oldSkipLists, list);
                    Interlocked.Exchange(ref _skipList, _skipListFunc());
                    await _ioScheduler.ScheduleSkipListSave(sl);
                }
            }
            finally
            {
                _skipListLock.Release();
            }
        }
    }
}

