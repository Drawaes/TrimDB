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
        public const int BlockSize = 4 << 10;
        public byte[] _data = new byte[BlockSize];

        [GlobalSetup]
        public void GlobalSetup()
        {
            var rand = new Random();
            rand.NextBytes(_data);
        }

        [Benchmark]
        public void Murmur64()
        {
            var hash = new MurmurHash3();
            hash.ComputeHash64(_data);
        }

        [Benchmark]
        public void Murmur32()
        {
            var hash = new MurmurHash3();
            hash.ComputeHash32(_data);
        }

        [Benchmark]
        public void Murmur128()
        {
            var hash = new MurmurHash3();
            hash.ComputeHash128(_data);
        }

        [Benchmark]
        public void Wyhash64()
        {
            WyHash64.ComputeHash64(_data);
        }

        [Benchmark]
        public void XXHash64()
        {
            xxHash64.ComputeHash(_data);
        }

        [Benchmark]
        public void XXHash32()
        {
            xxHash32.ComputeHash(_data);
        }
    }
}
