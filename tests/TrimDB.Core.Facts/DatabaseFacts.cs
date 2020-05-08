using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NuGet.Frameworks;
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
            var folder = "c:\\code\\trimdb\\Database";
            foreach (var f in System.IO.Directory.GetFiles(folder))
            {
                System.IO.File.Delete(f);
            }

            var db = new TrimDatabase(() => new SkipList32(new NativeAllocator32(4096 * 1024, 25)), 2, folder);

            foreach (var word in loadedWords)
            {
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await db.PutAsync(utf8, value);
            }

            var key = Encoding.UTF8.GetBytes(loadedWords[0]);
            var expectedValue = Encoding.UTF8.GetBytes($"VALUE={loadedWords[0]}");

            await Task.Delay(TimeSpan.FromSeconds(10));

            var result = await db.GetAsync(key);

            Assert.Equal(expectedValue.ToArray(), result.ToArray());

            key = Encoding.UTF8.GetBytes(loadedWords[loadedWords.Length / 2]);
            expectedValue = Encoding.UTF8.GetBytes($"VALUE={loadedWords[loadedWords.Length / 2]}");
            result = await db.GetAsync(key);

            Assert.Equal(expectedValue.ToArray(), result.ToArray());

            key = Encoding.UTF8.GetBytes(loadedWords[loadedWords.Length - 1]);
            expectedValue = Encoding.UTF8.GetBytes($"VALUE={loadedWords[loadedWords.Length - 1]}");
            result = await db.GetAsync(key);

            Assert.Equal(expectedValue.ToArray(), result.ToArray());


            //foreach (var word in loadedWords)
            //{
            //    var utf8 = Encoding.UTF8.GetBytes(word);
            //    var value = Encoding.UTF8.GetBytes($"VALUE={word}");
            //    await db.PutAsync(utf8, value);
            //}

            await Task.Delay(TimeSpan.FromSeconds(10));
        }
    }
}
