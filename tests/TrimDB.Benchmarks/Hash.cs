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
        public const int blockSize = 4 << 10;
        public byte[] data = new byte[blockSize];

        [GlobalSetup]
        public void GlobalSetup()
        {
            var rand = new Random();
            rand.NextBytes(data);
        }

        [Benchmark]
        public void Murmur64()
        {
            var hash = new MurmurHash3();
            hash.ComputeHash64(data);
        }

        [Benchmark]
        public void Murmur32()
        {
            var hash = new MurmurHash3();
            hash.ComputeHash32(data);
        }

        [Benchmark]
        public void Murmur128()
        {
            var hash = new MurmurHash3();
            hash.ComputeHash128(data);
        }

        [Benchmark]
        public void Wyhash64()
        {
            WyHash64.ComputeHash64(data);
        }

        [Benchmark]
        public void XXHash64()
        {
            xxHash64.ComputeHash(data);
        }

        [Benchmark]
        public void XXHash32()
        {
            xxHash32.ComputeHash(data);
        }
    }
}
