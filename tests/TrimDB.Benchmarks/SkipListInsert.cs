using System;
using System.CodeDom.Compiler;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using TrimDB.Core.SkipList;

namespace TrimDB.Benchmarks
{
    [MemoryDiagnoser]
    //[SimpleJob(RuntimeMoniker.NetCoreApp31, launchCount: 1, warmupCount: 2, targetCount: 1, invocationCount: 10, baseline: false)]
    public class SkipListInsert
    {
        private static byte[][] s_inputData;
        private static ConcurrentQueue<byte[]> s_job;
        private static readonly Random s_rnd = new Random(7777);

        [GlobalSetup]
        public void GlobalSetup()
        {
            s_inputData = System.IO.File.ReadAllLines("words.txt").Select(w => Encoding.UTF8.GetBytes(w)).ToArray();
            Shuffle(s_inputData);
        }

        private static void Shuffle<T>(T[] array)
        {
            var n = array.Length;
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
            using var simpleAllocator = new NativeAllocator(4096 * 1024 * 2, TableHeight);
            var skipList = new SkipList(simpleAllocator);
            var tasks = new Task[Environment.ProcessorCount];

            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => ThreadedPut(skipList));
            }
            await Task.WhenAll(tasks);
        }

        private static void ThreadedPut(SkipList skipList)
        {
            while (s_job.TryDequeue(out var value))
            {
                skipList.Put(value, value);
            }
        }
    }
}
