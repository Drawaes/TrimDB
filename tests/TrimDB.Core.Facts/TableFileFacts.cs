using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.InMemory.SkipList64;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.AsyncCache;
using TrimDB.Core.Storage.Blocks.CachePrototype;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

#pragma warning disable CS0618 // Obsolete SkipList64 types used in tests

namespace TrimDB.Core.Facts
{
    public class TableFileFacts
    {
        [Fact]
        public async Task WriteAndReadAsyncBlockFile()
        {
            using var allocator = new NativeAllocator64(4096 * 20_000, 25);
            var memoryTable = new SkipList64(allocator);

            var loadedWords = CommonData.Words;
            foreach (var word in loadedWords)
            {
                if (string.IsNullOrEmpty(word)) continue;
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                Assert.True(memoryTable.Put(utf8, value));
            }

            var tempPath = System.IO.Path.GetTempPath();
            var fileName = System.IO.Path.Combine(tempPath, "Level1_1.trim");
            System.IO.File.Delete(fileName);

            var fw = new TableFileWriter(fileName);
            await fw.SaveMemoryTable(memoryTable);

            using (var blockCache = new ProtoBlockCache(10000))
            {
                var loadedTable = new TableFile(fileName, blockCache);
                await loadedTable.LoadAsync();

                // Check we can get the values back out

                var hash = new MurmurHash3();
                foreach (var word in loadedWords)
                {
                    var utf8 = Encoding.UTF8.GetBytes(word);
                    var h = hash.ComputeHash64(utf8);

                    var result = await loadedTable.GetAsync(utf8, h);
                    var resultAsString = Encoding.UTF8.GetString(result.Value.Span);

                    result.Result.Should().Be(SearchResult.Found);
                    resultAsString.Should().EndWith(word);
                }
            }
        }

        [Fact]
        public async Task WriteAndReadFile()
        {
            using var allocator = new NativeAllocator64(4096 * 20_000, 25);
            var memoryTable = new SkipList64(allocator);

            var loadedWords = CommonData.Words;
            foreach (var word in loadedWords)
            {
                if (string.IsNullOrEmpty(word)) continue;
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                memoryTable.Put(utf8, value);
            }

            var tempPath = System.IO.Path.GetTempPath();
            var fileName = System.IO.Path.Combine(tempPath, "Level1_1.trim");
            System.IO.File.Delete(fileName);

            var fw = new TableFileWriter(fileName);
            await fw.SaveMemoryTable(memoryTable);

            using (var blockCache = new MMapBlockCache())
            {
                var loadedTable = new TableFile(fileName, blockCache);
                await loadedTable.LoadAsync();

                // Check we can get the values back out

                var hash = new MurmurHash3();
                foreach (var word in loadedWords)
                {
                    var utf8 = Encoding.UTF8.GetBytes(word);
                    var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                    var h = hash.ComputeHash64(utf8);

                    var result = await loadedTable.GetAsync(utf8, h);
                    Assert.Equal(SearchResult.Found, result.Result);
                    Assert.Equal(value, result.Value.ToArray());
                }
            }
            System.IO.File.Delete(fileName);

        }


        [Fact]
        public async Task CheckTableIteratorWorks()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 20_000, 25);
            var memoryTable = new SkipList32(allocator);

            var loadedWords = CommonData.Words;
            var insertedCount = 0;
            foreach (var word in loadedWords)
            {
                if (string.IsNullOrEmpty(word)) continue;
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                if (memoryTable.Put(utf8, value)) insertedCount++;
            }

            var tempPath = System.IO.Path.GetTempPath();
            var fileName = System.IO.Path.Combine(tempPath, "Level2_2.trim");
            System.IO.File.Delete(fileName);

            var fw = new TableFileWriter(fileName);
            await fw.SaveMemoryTable(memoryTable);

            using (var blockCache = new MMapBlockCache())
            {
                var loadedTable = new TableFile(fileName, blockCache);
                await loadedTable.LoadAsync();

                var count = 0;
                await foreach (var item in loadedTable)
                {
                    count++;
                    var key = Encoding.UTF8.GetString(item.Key);
                    var value = Encoding.UTF8.GetString(item.Value);

                    Assert.Equal($"VALUE={key}", value);
                }
                Assert.Equal(insertedCount, count);
            }
            System.IO.File.Delete(fileName);
        }
    }
}
