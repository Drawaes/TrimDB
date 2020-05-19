using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks.CachePrototype;

namespace TrimDB.Benchmarks
{
    public class BlockReaderBench
    {
        private ProtoBlockCache _cache;
        private TableFile _file;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _cache = new ProtoBlockCache(10);
            var tempPath = System.IO.Path.GetTempPath();
            var fileName = System.IO.Path.Combine(tempPath, "Level1_1.trim");
            _file = new TableFile(fileName, _cache);
            _file.LoadAsync().Wait();
        }

        [Benchmark]
        public async Task ReadBlock()
        {
            var block = await _file.GetKVBlock(0);
            var count = 0;
            while (block.TryGetNextKeySlow(out _))
            {
                count++;
            }
        }

        [Benchmark(Baseline = true)]
        public async Task LessSpan()
        {
            var block = await _file.GetKVBlock(0);
            var count = 0;
            while (block.TryGetNextKey(out _))
            {
                count++;
            }
        }

        [Benchmark]
        public async Task LessGetSpan()
        {
            var block = await _file.GetKVBlock(0);
            var count = 0;
            while (block.TryGetNextKey2(out _))
            {
                count++;
            }
        }
    }
}
