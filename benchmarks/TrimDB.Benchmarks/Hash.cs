using System;
using System.Collections.Generic;
using System.Text;
using BenchmarkDotNet.Attributes;
using Standart.Hash.xxHash;
using TrimDB.Core.Hashing;
using WyHash;

namespace TrimDB.Benchmarks
{
    public class Hash
    {
        public byte[] _data;
        private static readonly Random _rand = new Random(7722);

        [Params(10)]
        public int BlockSize { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _data = new byte[BlockSize];
            _rand.NextBytes(_data);
        }

        [Benchmark(Baseline = true)]
        public void Murmur64()
        {
            var hash = new MurmurHash3();
            hash.ComputeHash64(_data);
        }

        //[Benchmark]
        //public void Wyhash64()
        //{
        //    WyHash64.ComputeHash64(_data);
        //}

        [Benchmark]
        public void XXHash64()
        {
            xxHash64.ComputeHash(_data);
        }

        [Benchmark]
        public void FarmHash64()
        {
            Farmhash.Sharp.Farmhash.Hash64(_data);
        }

    }
}
