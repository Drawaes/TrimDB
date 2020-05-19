using System;
using System.Diagnostics;
using System.IO;
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
            //var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
            //return;

            Console.WriteLine("Are you ready?");
            Console.ReadLine();

            Console.WriteLine("Started");
            var loadedWords = await File.ReadAllLinesAsync("words.txt");

            var wordSpans = loadedWords.Select(w => Encoding.UTF8.GetBytes(w)).ToArray();

            var tempPath = Path.GetTempPath();
            var fileName = Path.Combine(tempPath, "Level1_1.trim");


            var sw = Stopwatch.StartNew();

            await WriteAndReadAsyncBlockFile(fileName, wordSpans);

            Console.WriteLine($"This took {sw.ElapsedMilliseconds}ms");


        }

        public static async Task WriteAndReadAsyncBlockFile(string fileName, byte[][] wordSpans)
        {
            using (var blockCache = new ProtoSharded(200))
            {
                var loadedTable = new TableFile(fileName, blockCache);
                await loadedTable.LoadAsync();

                var block = await blockCache.GetBlock(new Core.Storage.Blocks.FileIdentifier(1, 1), 0);

                using (var fs = new StreamWriter("C:\\code\\trimdb\\array.txt"))
                {
                    for (var i = 0; i < block.Memory.Length; i++)
                    {
                        fs.Write($"{block.Memory.Span[i]},");
                    }
                }

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
