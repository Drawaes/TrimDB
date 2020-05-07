using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.InMemory.SkipList64;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DatabaseFacts
    {
        [Fact]
        public async Task TestSkipListOverflow()
        {
            var loadedWords = await System.IO.File.ReadAllLinesAsync("words.txt");
            var db = new TrimDatabase(() => new SkipList32(new NativeAllocator32(4096 * 1024, 25)), 2, "c:\\code\\trimdb\\Database");

            foreach (var word in loadedWords)
            {
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await db.PutAsync(utf8, value);
            }
        }
    }
}
