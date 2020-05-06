using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.SkipList;
using TrimDB.Core.Storage;
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

            foreach (var word in loadedWords)
            {
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                skipList.Put(utf8, value);
            }

            var filePath = "C:\\code\\trimdb\\test.trim";

            var writer = new TableFileWriter(filePath);
            await writer.SaveSkipList(skipList);

            var reader = new TableFile(writer.FileName);
            await reader.LoadAsync();
        }
        //    bytes.Sort(Compare);

        //    for (var t = 0; t < threads.Length; t++)
        //    {
        //        var start = t * Nodes;
        //        var end = start + Nodes;

        //        var task = Task.Run(() => RunPut(start, end, skipList, bytes));
        //        threads[t] = task;
        //    }

        //    await Task.WhenAll(threads);
        //    CompareInOrder(bytes, skipList, Nodes, threads);
        //}

        //private void RunPut(int start, int end, SkipList.SkipList skipList, List<byte[]> bytes)
        //{
        //    for (var i = start; i < end; i++)
        //    {
        //        var data = bytes[i];
        //        skipList.Put(data, data);
        //    }
        //}

        //private int Compare(byte[] valueA, byte[] valueB)
        //{
        //    return valueA.AsSpan().SequenceCompareTo(valueB);
        //}

        //private static void CompareInOrder(List<byte[]> loadedWords, SkipList.SkipList skipList, int nodes, Task[] threads)
        //{
        //    var iter = skipList.GetIterator();
        //    _ = iter.GetNext();

        //    var newWords = new List<byte[]>();

        //    for (var i = 0; i < nodes * threads.Length; i++)
        //    {
        //        var node = iter.GetNext();
        //        newWords.Add(node.Key.ToArray());
        //    }

        //    Assert.Equal(loadedWords.Take(newWords.Count), newWords);
        //}

        [Fact]
        public void SkipListPutWorking()
        {
            var string1 = Encoding.UTF8.GetBytes("This is the first test string");
            var string2 = Encoding.UTF8.GetBytes("This is the second test string");
            var string3 = Encoding.UTF8.GetBytes("This is the missing test string");
            var value1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var value2 = new byte[] { 10, 11, 12, 13, 14, 15 };
            _ = new byte[] { 16, 17, 18, 19, 20 };

            var allocator = new NativeAllocator(4096, 5);
            var skipList = new SkipList.SkipList(allocator);

            skipList.Put(string1, value1);
            skipList.Put(string2, value2);
            var result = skipList.TryGet(string1, out var valueResult);
            Assert.Equal(SearchResult.Found, result);
            Assert.Equal(value1, valueResult.ToArray());

            var result2 = skipList.TryGet(string2, out _);
            Assert.Equal(SearchResult.Found, result2);
            var result3 = skipList.TryGet(string3, out _);
            Assert.Equal(SearchResult.NotFound, result3);
        }

        
    }
}
