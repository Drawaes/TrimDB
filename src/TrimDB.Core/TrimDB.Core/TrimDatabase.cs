using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Hashing;
using TrimDB.Core.SkipList;
using TrimDB.Core.Storage;

namespace TrimDB.Core
{
    public class TrimDatabase
    {
        private SkipList.SkipList _skipList;
        private Func<SkipList.SkipList> _skipListFunc;
        private SkipList.SkipList _oldSkipList;
        private List<StorageLayer> _storageLayers = new List<StorageLayer>();
        private IHashFunction _hasher = new MurmurHash3();

        public TrimDatabase(Func<SkipList.SkipList> skipListFunc, int levels)
        {
            _skipListFunc = skipListFunc;

            _storageLayers.Add(new UnsortedStorageLayer());
            for (var i = 1; i < levels; i++)
            {
                _storageLayers.Add(new SortedStorageLayer(i));
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

            if (_oldSkipList != null)
            {
                sl = _oldSkipList;
                result = sl.TryGet(key, out value);
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

        public void PutAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {

        }

        public void PutAsync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, long oldValueLocation)
        {

        }

        public void RangeAsync()
        {
            throw new NotImplementedException();
        }
    }
}
