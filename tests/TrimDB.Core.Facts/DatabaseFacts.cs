using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DatabaseFacts : IAsyncLifetime
    {
        private readonly string _folder;
        private TrimDatabase _db;

        public DatabaseFacts()
        {
            _folder = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "TrimDB_Tests_" + Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(_folder);
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            if (_db != null)
            {
                try { await _db.DisposeAsync(); } catch { }
            }
            try
            {
                if (System.IO.Directory.Exists(_folder))
                    System.IO.Directory.Delete(_folder, true);
            }
            catch { }
        }

        [Fact]
        public async Task TestSkipListOverflow()
        {
            var loadedWords = CommonData.Words;

            var dbOptions = new TrimDatabaseOptions() { DatabaseFolder = _folder };
            _db = new TrimDatabase(dbOptions);

            await _db.LoadAsync();

            foreach (var word in loadedWords)
            {
                var utf8 = Encoding.UTF8.GetBytes(word);
                var value = Encoding.UTF8.GetBytes($"VALUE={word}");
                await _db.PutAsync(utf8, value);
            }

            var key = Encoding.UTF8.GetBytes(loadedWords[0]);
            var expectedValue = Encoding.UTF8.GetBytes($"VALUE={loadedWords[0]}");

            var result = await _db.GetAsync(key);

            Assert.Equal(expectedValue.ToArray(), result.ToArray());

            key = Encoding.UTF8.GetBytes(loadedWords[loadedWords.Length / 2]);
            expectedValue = Encoding.UTF8.GetBytes($"VALUE={loadedWords[loadedWords.Length / 2]}");
            result = await _db.GetAsync(key);

            Assert.Equal(expectedValue.ToArray(), result.ToArray());

            key = Encoding.UTF8.GetBytes(loadedWords[^1]);
            expectedValue = Encoding.UTF8.GetBytes($"VALUE={loadedWords[^1]}");
            result = await _db.GetAsync(key);

            Assert.Equal(expectedValue.ToArray(), result.ToArray());
        }
    }
}
