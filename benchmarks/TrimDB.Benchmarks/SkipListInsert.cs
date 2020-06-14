using System;
using System.CodeDom.Compiler;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.InMemory.SkipList64;

namespace TrimDB.Benchmarks
{
    [MemoryDiagnoser]
    //[SimpleJob(RuntimeMoniker.NetCoreApp31, launchCount: 1, warmupCount: 2, targetCount: 1, invocationCount: 10, baseline: false)]
    public class SkipListInsert
    {
        private static readonly List<byte[]> s_inputData = new List<byte[]>();
        private static ConcurrentQueue<byte[]> s_job;
        private static readonly Random s_rnd = new Random(7777);

        [GlobalSetup]
        public void GlobalSetup()
        {
            var randomBytes = new byte[8];
            foreach (var line in System.IO.File.ReadAllLines("words.txt").Select(w => Encoding.UTF8.GetBytes(w)).ToArray())
            {
                s_inputData.Add(line);

                s_rnd.NextBytes(randomBytes);
                for (var i = 0; i < 10; i++)
                {
                    s_inputData.Add(randomBytes.Concat(line).ToArray());
                }
            }
            Shuffle(s_inputData);
        }

        private static void Shuffle<T>(List<T> array)
        {
            var n = array.Count;
            while (n > 1)
            {
                var k = s_rnd.Next(n--);
                var t = array[n];
                array[n] = array[k];
                array[k] = t;
            }
        }

        [IterationSetup]
        public void IterationSetup() => s_job = new ConcurrentQueue<byte[]>(s_inputData);

        [Params(20)]
        public byte TableHeight { get; set; }

        [Benchmark(Baseline = true)]
        public async Task NativeAllocator()
        {
            using var simpleAllocator = new NativeAllocator64(4096 * 1024 * 10, TableHeight);
            var skipList = new SkipList64(simpleAllocator);
            var tasks = new Task[Environment.ProcessorCount];

            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => ThreadedPut(skipList));
            }
            await Task.WhenAll(tasks);
        }

        [Benchmark]
        public async Task ArrayAllocator()
        {
            using var simpleAllocator = new ArrayBasedAllocator64(4096 * 1024 * 10, TableHeight);
            var skipList = new SkipList64(simpleAllocator);
            var tasks = new Task[Environment.ProcessorCount];

            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => ThreadedPut(skipList));
            }
            await Task.WhenAll(tasks);
        }

        [Benchmark()]
        public async Task Native32Allocator()
        {
            using var simpleAllocator = new ArrayBasedAllocator32(4096 * 1024 * 10, TableHeight);
            var skipList = new SkipList32(simpleAllocator);
            var tasks = new Task[Environment.ProcessorCount];

            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => ThreadedPut(skipList));
            }
            await Task.WhenAll(tasks);
        }


        //[Benchmark]
        //public async Task ConcurrentDictionaryNoCopy()
        //{
        //    var dictionary = new ConcurrentDictionary<byte[], byte[]>(new Comparer());
        //    var tasks = new Task[Environment.ProcessorCount];

        //    for (var i = 0; i < tasks.Length; i++)
        //    {
        //        tasks[i] = Task.Run(() => ConcurrentDictionaryPutNoCopy(dictionary));
        //    }
        //    await Task.WhenAll(tasks);
        //}

        private class Comparer : IEqualityComparer<byte[]>
        {
            public bool Equals(byte[] x, byte[] y)
            {
                return x.AsSpan().SequenceCompareTo(y) == 0;
            }

            public int GetHashCode(byte[] obj)
            {
                return (int)CalculateCRC2(0, obj);
            }

            private uint CalculateCRC2(uint crc, ReadOnlySpan<byte> span)
            {
                ref var mem = ref MemoryMarshal.GetReference(span);
                var remaining = span.Length;

                if ((remaining & 0x01) == 1)
                {
                    crc = System.Runtime.Intrinsics.X86.Sse42.Crc32(crc, mem);
                    mem = ref Unsafe.Add(ref mem, 1);
                    remaining--;
                }

                if ((remaining & 0x02) == 2)
                {
                    crc = System.Runtime.Intrinsics.X86.Sse42.Crc32(crc, Unsafe.As<byte, ushort>(ref mem));
                    mem = ref Unsafe.Add(ref mem, 2);
                    remaining -= 2;
                }

                ref var uints = ref Unsafe.As<byte, uint>(ref mem);
                while (remaining > 0)
                {
                    crc = System.Runtime.Intrinsics.X86.Sse42.Crc32(crc, uints);
                    uints = ref Unsafe.Add(ref uints, 1);
                    remaining -= 4;
                }

                return crc;
            }
        }


        //[Benchmark]
        public async Task ConcurrentDictionary()
        {
            var dictionary = new ConcurrentDictionary<byte[], byte[]>(new Comparer());
            var tasks = new Task[Environment.ProcessorCount];

            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => ConcurrentDictionaryPut(dictionary));
            }
            await Task.WhenAll(tasks);

        }

        private static void ConcurrentDictionaryPut(ConcurrentDictionary<byte[], byte[]> dictionary)
        {
            while (s_job.TryDequeue(out var value))
            {
                var copy = new byte[value.Length];
                Array.Copy(value, copy, value.Length);
                dictionary.TryAdd(copy, copy);
            }
        }

        private static void ConcurrentDictionaryPutNoCopy(ConcurrentDictionary<byte[], byte[]> dictionary)
        {
            while (s_job.TryDequeue(out var value))
            {
                dictionary.TryAdd(value, value);
            }
        }

        private static void ThreadedPut(MemoryTable skipList)
        {
            while (s_job.TryDequeue(out var value))
            {
                skipList.Put(value, value);
            }
        }

    }
}
