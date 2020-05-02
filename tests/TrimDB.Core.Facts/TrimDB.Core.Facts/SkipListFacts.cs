using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.SkipList;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class SkipListFacts
    {
        [Fact]
        public async Task TestCanPutInOrder()
        {
            var loadedWords = await System.IO.File.ReadAllLinesAsync("words.txt");
            using var simpleAllocator = new NativeAllocator(4096 * 10_000, 25);
            var skipList = new SkipList.SkipList(simpleAllocator);
            var nodes = 1_000;
            var threads = new Task[Environment.ProcessorCount];
            var bytes = loadedWords.Select(lw => Encoding.UTF8.GetBytes(lw)).ToList();
            bytes.Sort(Compare);

            for (var t = 0; t < threads.Length; t++)
            {
                var start = t * nodes;
                var end = start + nodes;

                var task = Task.Run(() => RunPut(start, end, skipList, bytes));
                threads[t] = task;
            }

            await Task.WhenAll(threads);
            CompareInOrder(bytes, skipList, nodes, threads);
        }

        private void RunPut(int start, int end, SkipList.SkipList skipList, List<byte[]> bytes)
        {
            for (var i = start; i < end; i++)
            {
                var data = bytes[i];
                skipList.Put(data, data);
            }
        }

        private int Compare(byte[] valueA, byte[] valueB)
        {
            return valueA.AsSpan().SequenceCompareTo(valueB);
        }

        private static void CompareInOrder(List<byte[]> loadedWords, SkipList.SkipList skipList, int nodes, Task[] threads)
        {
            var iter = skipList.GetIterator();
            var node = iter.GetNext();

            var newWords = new List<byte[]>();

            for (var i = 0; i < nodes * threads.Length; i++)
            {
                node = iter.GetNext();
                newWords.Add(node.Key.ToArray());
            }

            Assert.Equal(loadedWords.Take(newWords.Count), newWords);
        }

        [Fact]
        public void SkipListPutWorking()
        {
            var string1 = Encoding.UTF8.GetBytes("This is the first test string");
            var string2 = Encoding.UTF8.GetBytes("This is the second test string");
            var string3 = Encoding.UTF8.GetBytes("This is the missing test string");
            var value1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var value2 = new byte[] { 10, 11, 12, 13, 14, 15 };
            var value3 = new byte[] { 16, 17, 18, 19, 20 };

            var allocator = new NativeAllocator(4096, 5);
            var skipList = new SkipList.SkipList(allocator);

            skipList.Put(string1, value1);
            skipList.Put(string2, value2);

            var result = skipList.TryGet(string1, out var rValue1);
            Assert.Equal(SkipList.SkipList.SkipListResult.Found, result);

            var result2 = skipList.TryGet(string2, out var rValue2);
            Assert.Equal(SkipList.SkipList.SkipListResult.Found, result2);

            var result3 = skipList.TryGet(string3, out var rValue3);
            Assert.Equal(SkipList.SkipList.SkipListResult.NotFound, result3);
        }
    }
}
