using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualBasic.CompilerServices;
using TrimDB.Core;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.InMemory.SkipList64;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks.CachePrototype;
using TrimDB.Core.Storage.Layers;

namespace TrimDB.DatabaseStress
{
    class Program
    {
        private static readonly int _keySize = 10;
        private static readonly int _valueSize = 100;
        private static readonly int _keysPerThread = 100_000;

        static async Task Main(string[] args)
        {
            var dbFolder = "D:\\stress";


            await WriteDB(dbFolder);

            //await CheckGetKeys(dbFolder);

            //await CheckGetKeysSingleThread(dbFolder);

            //await CheckLayer(dbFolder, 2);

            //await SpeedTestSingleThreadedSearchFile();
        }

        private static async Task PracticeL2ToL3Merge(string dbFolder)
        {
            using var blockStore = new ProtoSharded(2_560);

            var l2 = new SortedStorageLayer(2, dbFolder, blockStore, 0, 0);

            var l3 = new SortedStorageLayer(3, dbFolder, blockStore, 0, 0);
            MergeInFile(l2, l3);

        }

        private static void MergeInFile(SortedStorageLayer l2, SortedStorageLayer l3)
        {

            if (l3.NumberOfTables == 0)
            {
                Console.WriteLine("Moved table down with no merge");
                // Move table down one level
                return;
            }
            var overlapCounts = new int[l2.NumberOfTables];
            for (var i = 0; i < l2.GetTables().Length; i++)
            {
                var t = l2.GetTables()[i];

                // Check if there is overlap
                var overlapCount = 0;

                foreach (var l3t in l3.GetTables())
                {
                    if (t.LastKey.Span.SequenceCompareTo(l3t.FirstKey.Span) < 0)
                    {
                        continue;
                    }
                    if (t.FirstKey.Span.SequenceCompareTo(l3t.LastKey.Span) > 0)
                    {
                        continue;
                    }
                    overlapCount++;
                    break;
                }

                if (overlapCount == 0)
                {
                    // Move table down one level
                    Console.WriteLine("Moved table down with no merge");
                    return;
                }
                overlapCounts[i] = overlapCount;
            }

            var min = overlapCounts.Min();
            var indexOfMin = overlapCounts.Select((value, index) => (value, index)).First(i => i.value == min).index;

            // Found with min overlap so merge it
            return;
        }

        private static async Task SpeedTestSingleThreadedSearchFile()
        {
            var tempPath = System.IO.Path.GetTempPath();
            var fileName = System.IO.Path.Combine(tempPath, "Level1_1.trim");
            var loadedWords = await System.IO.File.ReadAllLinesAsync("words.txt");


            using (var blockCache = new ProtoBlockCache(10000))
            {
                var loadedTable = new TableFile(fileName, blockCache);
                await loadedTable.LoadAsync();

                // Check we can get the values back out

                var hash = new MurmurHash3();

                var sw = Stopwatch.StartNew();
                foreach (var word in loadedWords)
                {
                    var utf8 = Encoding.UTF8.GetBytes(word);
                    var h = hash.ComputeHash64(utf8);

                    var result = await loadedTable.GetAsync(utf8, h);
                    var resultAsString = Encoding.UTF8.GetString(result.Value.Span);
                }
                sw.Stop();
                Console.WriteLine($"Total time taken {sw.ElapsedMilliseconds} time per key {(double)sw.ElapsedMilliseconds / loadedWords.Length}");
            }
        }

        private static async Task CheckGetKeysSingleThread(string dbFolder)
        {
            var dbOptions = new TrimDatabaseOptions() { DatabaseFolder = dbFolder, OpenReadOnly = true };

            await using var db = new TrimDatabase(dbOptions);
            await db.LoadAsync();

            var numberOfThreads = Environment.ProcessorCount;
            var seed = 7722;

            var generator = new KeyValueGenerator(numberOfThreads, seed);

            var key = new byte[10];
            var value = new byte[100];

            for (var t = 0; t < numberOfThreads; t++)
            {
                for (var i = 0; i < _keysPerThread; i++)
                {
                    //if (i == 7) Debugger.Break();
                    generator.GetKeyValue(key, value, (short)t, i);
                    Console.WriteLine($"Thread Id {t} iteration {i}");
                    await db.GetAsync(key);
                }
            }

        }

        private static async Task CheckGetKeys(string dbFolder)
        {
            var dbOptions = new TrimDatabaseOptions() { DatabaseFolder = dbFolder, OpenReadOnly = true };
            await using var db = new TrimDatabase(dbOptions);

            await db.LoadAsync();

            var numberOfThreads = Environment.ProcessorCount;

            var tasks = new Task[numberOfThreads];
            var seed = 7722;

            var generator = new KeyValueGenerator(numberOfThreads, seed);

            Console.WriteLine("Starting the get test");

            var sw = Stopwatch.StartNew();

            for (var i = 0; i < numberOfThreads; i++)
            {
                tasks[i] = ReadFromDB((short)i, generator, _keysPerThread, db);
            }

            await Task.WhenAll(tasks);

            sw.Stop();

            Console.WriteLine($"Total time taken {sw.ElapsedMilliseconds}ms");
            var timePerKey = (double)sw.ElapsedMilliseconds / (_keysPerThread * numberOfThreads);
            Console.WriteLine($"Time taken per key {timePerKey * 1000.0}µs");
            Console.WriteLine($"Total misses {_numberOfMisses}");
        }

        private static int _numberOfMisses;

        private static async Task ReadFromDB(short threadId, KeyValueGenerator generator, int numberOfIterations, TrimDatabase trimDB)
        {
            await Task.Yield();

            var keyMemory = new byte[_keySize];
            var valueMemory = new byte[_valueSize];

            for (var i = 0; i < numberOfIterations; i++)
            {
                generator.GetKeyValue(keyMemory.AsSpan(), valueMemory.AsSpan(), threadId, numberOfIterations);
                var result = await trimDB.GetAsync(keyMemory);
                if (result.IsEmpty) Interlocked.Increment(ref _numberOfMisses);
                //if ((i + 1) % 10 == 0) Console.WriteLine($"Thread {threadId} has read {i + 1} keys");
                //if (threadId == 3 && i > 228) Debugger.Break(); // Console.WriteLine($"ERROR Thread {threadId} has read {i + 1} keys");
            }
        }

        private static async Task CheckLayer(string dbFolder, int level)
        {
            using var blockStore = new ProtoSharded(2_560);

            var sortedLayer = new SortedStorageLayer(level, dbFolder, blockStore, 1024 * 1024 * 6, 0);

            await sortedLayer.LoadLayer();

            var firstLast = sortedLayer.GetFirstAndLastKeys().ToArray();

            for (var outer = 0; outer < firstLast.Length; outer++)
            {
                for (var inner = 0; inner < firstLast.Length; inner++)
                {
                    if (inner == outer) continue;
                    var i = firstLast[inner];
                    var o = firstLast[outer];

                    var compareStart = i.firstKey.Span.SequenceCompareTo(o.firstKey.Span);

                    if (compareStart < 0)
                    {
                        // The start is before the block so overlap if the finish is after the
                        // next start
                        var compareFinish = i.lastKey.Span.SequenceCompareTo(o.firstKey.Span);
                        if (compareFinish > 0) throw new InvalidOperationException("We found an overlap");
                    }

                    if (compareStart > 0)
                    {
                        // The start is after the start of the outer block if the start is before
                        // the end then there is an overlap
                        var compareFinish = i.firstKey.Span.SequenceCompareTo(o.lastKey.Span);
                        if (compareFinish < 0) throw new InvalidOperationException("We found an overlap");
                    }
                }
            }
        }

        private static async Task WriteDB(string dbFolder)
        {
            System.IO.Directory.Delete(dbFolder, true);

            System.IO.Directory.CreateDirectory(dbFolder);

            var dbOptions = new TrimDatabaseOptions() { DatabaseFolder = dbFolder };
            await using var db = new TrimDatabase(dbOptions);

            await db.LoadAsync();

            var numberOfThreads = Environment.ProcessorCount;

            var tasks = new Task[numberOfThreads];
            var seed = 7722;

            var generator = new KeyValueGenerator(numberOfThreads, seed);

            var sw = Stopwatch.StartNew();

            for (var i = 0; i < numberOfThreads; i++)
            {
                tasks[i] = WriteToDB((short)i, generator, _keysPerThread, db);
            }

            await Task.WhenAll(tasks);

            sw.Stop();

            Console.WriteLine($"Total time taken {sw.ElapsedMilliseconds}ms");

            Console.WriteLine($"Total number of keys written {_keysPerThread * numberOfThreads}");
            var totalDatasize = _keysPerThread * numberOfThreads * (_keySize + _valueSize);
            Console.WriteLine($"Total data set {totalDatasize / 1024 / 1024 }mb");

            Console.WriteLine("Waiting for db to shutdown");

            //for (var i = 0; i < 10; i++)
            //{
            //    Console.WriteLine("Waiting for the merges to finish BRB");
            //    await Task.Delay(TimeSpan.FromSeconds(10));
            //}
        }

        private static async Task WriteToDB(short threadId, KeyValueGenerator generator, int numberOfIterations, TrimDatabase trimDB)
        {
            await Task.Yield();

            var keyMemory = new byte[_keySize];
            var valueMemory = new byte[_valueSize];

            for (var i = 0; i < numberOfIterations; i++)
            {

                generator.GetKeyValue(keyMemory.AsSpan(), valueMemory.AsSpan(), threadId, numberOfIterations);
                await trimDB.PutAsync(keyMemory, valueMemory);
            }
        }
    }
}
