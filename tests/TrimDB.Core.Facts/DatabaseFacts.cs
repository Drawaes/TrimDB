using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.SkipList;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DatabaseFacts
    {
        [Fact]
        public async Task TestSkipListOverflow()
        {
            var loadedWords = await System.IO.File.ReadAllLinesAsync("words.txt");
            var db = new TrimDatabase(() => new SkipList.SkipList(new NativeAllocator(4096 * 1024, 25)), 2, "c:\\code\\trimdb\\Database");

            foreach (var word in loadedWords)
            {
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await db.PutAsync(utf8, value);
            }
        }
    }
}
