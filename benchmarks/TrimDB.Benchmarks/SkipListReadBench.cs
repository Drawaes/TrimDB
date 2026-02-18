using System;
using System.Text;
using BenchmarkDotNet.Attributes;
using TrimDB.Core;
using TrimDB.Core.InMemory.SkipList32;

namespace TrimDB.Benchmarks
{
    [Config(typeof(TrimBenchConfig))]
    public class SkipListReadBench
    {
        private SkipList32 _skipList = null!;
        private byte[] _hitKey = null!;
        private byte[] _missKey = null!;
        private const int KeyCount = 10_000;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var allocator = new ArrayBasedAllocator32(64 * 1024 * 1024, 25);
            _skipList = new SkipList32(allocator);

            for (int i = 0; i < KeyCount; i++)
            {
                var key = Encoding.UTF8.GetBytes($"skiplist_key_{i:D8}");
                var value = Encoding.UTF8.GetBytes($"skiplist_value_{i:D8}");
                _skipList.Put(key, value);
            }

            _hitKey = Encoding.UTF8.GetBytes($"skiplist_key_{KeyCount / 2:D8}");
            _missKey = Encoding.UTF8.GetBytes("skiplist_key_zzzzzzzz_not_exist");
        }

        [Benchmark(Baseline = true)]
        public SearchResult TryGet_Hit()
        {
            return _skipList.TryGet(_hitKey, out _);
        }

        [Benchmark]
        public SearchResult TryGet_Miss()
        {
            return _skipList.TryGet(_missKey, out _);
        }

        [Benchmark]
        public SearchResult TryGetMemory_Hit()
        {
            return _skipList.TryGetMemory(_hitKey, out _);
        }

        [Benchmark]
        public bool Delete_Existing()
        {
            // Re-deleting an already-deleted key — exercises the tombstone hot path
            return _skipList.Delete(_hitKey);
        }
    }
}
