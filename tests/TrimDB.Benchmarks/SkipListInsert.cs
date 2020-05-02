using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using TrimDB.Core.SkipList;

namespace TrimDB.Benchmarks
{
    [MemoryDiagnoser]
    public class SkipListInsert
    {
        private static byte[][] _inputData;
        private static ConcurrentQueue<byte[]> _job;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _inputData = System.IO.File.ReadAllLines("words.txt").Select(w => Encoding.UTF8.GetBytes(w)).ToArray();

        }

        [IterationSetup]
        public void IterationSetup() => _job = new ConcurrentQueue<byte[]>(_inputData);

        //[Benchmark]
        //public int SingleThreaded()
        //{
        //    var simpleAllocator = new SimpleAllocator(1024 * 1024 * 5, 20);
        //    var skipList = new SkipList(simpleAllocator);
        //    var input = _inputData;
        //    for (var i = 0; i < input.Length; i++)
        //    {
        //        skipList.Put(input[i], input[i]);
        //    }
        //    return 1;
        //}

        [Params(20)]
        public byte TableHeight { get; set; }

        [Benchmark(Baseline = true)]
        public async Task MultiThreaded()
        {
            using var simpleAllocator = new SimpleAllocator(1024 * 1024 * 5, TableHeight);
            var skipList = new SkipList(simpleAllocator);
            var tasks = new Task[Environment.ProcessorCount];

            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => ThreadedPut(skipList));
            }
            await Task.WhenAll(tasks);
        }

        [Benchmark()]
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
            var job = _job;
            while (_job.TryDequeue(out var value))
            {
                skipList.Put(value, value);
            }
        }

        [Benchmark()]
        public async Task MultiThreadedNew()
        {
            using var simpleAllocator = new NativeAllocator(1024 * 1024 * 5, TableHeight);
            var skipList = new SkipListNew(simpleAllocator);
            var tasks = new Task[Environment.ProcessorCount];

            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => ThreadedPutNew(skipList));
            }
            await Task.WhenAll(tasks);
        }

        private static void ThreadedPutNew(SkipListNew skipList)
        {
            var job = _job;
            while (job.TryDequeue(out var value))
            {
                skipList.Put(value, value);
            }
        }
    }
}
