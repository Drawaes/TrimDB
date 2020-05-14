using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using TrimDB.Core;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks.CachePrototype;

namespace TrimDB.Benchmarks
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Are you ready?");
            
            var sw = Stopwatch.StartNew();
            Console.WriteLine("Started");
            await WriteAndReadAsyncBlockFile();

            Console.WriteLine($"This took {sw.ElapsedMilliseconds}ms");
            //var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }

        public static async Task WriteAndReadAsyncBlockFile()
        {
            var loadedWords = await System.IO.File.ReadAllLinesAsync("words.txt");

            var wordSpans = loadedWords.Select(w => Encoding.UTF8.GetBytes(w)).ToArray();

            var tempPath = System.IO.Path.GetTempPath();
            var fileName = System.IO.Path.Combine(tempPath, "Level1_1.trim");

            using (var blockCache = new ProtoSharded(200))
            {
                var loadedTable = new TableFile(fileName, blockCache);
                await loadedTable.LoadAsync();

                // Check we can get the values back out

                var hash = new MurmurHash3();
                var taskList = new Task[Environment.ProcessorCount];
                for (var i = 0; i < taskList.Length; i++)
                {
                    taskList[i] = Task.Run(async () =>
                    {
                        foreach (var word in wordSpans)
                        {
                            var h = hash.ComputeHash64(word);

                            var result = await loadedTable.GetAsync(word, h);

                            if (result.Result != SearchResult.Found)
                            {
                                throw new NotImplementedException();
                            }
                        }
                    });
                }

                await Task.WhenAll(taskList);
            }

            
        }
    }
}
