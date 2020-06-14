using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions.Common;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.InMemory.SkipList64;
using TrimDB.Core.Storage;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class SkipListFacts
    {
        [Fact]
        public Task CanPutInOrder32()
        {
            using var simpleAllocator = new ArrayBasedAllocator32(4096 * 50_000, 25);
            var skipList = new SkipList32(simpleAllocator);
            return CanPutInOrder(skipList);
        }

        [Fact]
        public Task CanPutInOrder64()
        {
            using var simpleAllocator = new ArrayBasedAllocator64(4096 * 50_000, 25);
            var skipList = new SkipList64(simpleAllocator);
            return CanPutInOrder(skipList);
        }

        [Fact]
        public void SkipListPutWorking32()
        {
            using var simpleAllocator = new ArrayBasedAllocator32(4096 * 10_000, 25);
            var skipList = new SkipList32(simpleAllocator);
            SkipListPutWorking(skipList);
        }

        [Fact]
        public void SkipListPutWorking64()
        {
            using var simpleAllocator = new ArrayBasedAllocator64(4096 * 10_000, 25);
            var skipList = new SkipList64(simpleAllocator);
            SkipListPutWorking(skipList);
        }

        private void SkipListPutWorking(MemoryTable memoryTable)
        {
            var string1 = Encoding.UTF8.GetBytes("This is the first test string");
            var string2 = Encoding.UTF8.GetBytes("This is the second test string");
            var string3 = Encoding.UTF8.GetBytes("This is the missing test string");
            var value1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var value2 = new byte[] { 10, 11, 12, 13, 14, 15 };
            _ = new byte[] { 16, 17, 18, 19, 20 };

            memoryTable.Put(string1, value1);
            memoryTable.Put(string2, value2);
            var result = memoryTable.TryGet(string1, out var valueResult);
            Assert.Equal(SearchResult.Found, result);
            Assert.Equal(value1, valueResult.ToArray());

            var result2 = memoryTable.TryGet(string2, out _);
            Assert.Equal(SearchResult.Found, result2);
            var result3 = memoryTable.TryGet(string3, out _);
            Assert.Equal(SearchResult.NotFound, result3);
        }

        private async Task CanPutInOrder(MemoryTable memoryTable)
        {
            var loadedWords = CommonData.Words;

            foreach (var word in loadedWords)
            {
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                var result = memoryTable.Put(utf8, value);
                Assert.True(result);
            }

            foreach(var word in loadedWords)
            {
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                var result = memoryTable.TryGet(utf8, out _);
                Assert.Equal(SearchResult.Found, result);
            }
        }
    }
}
