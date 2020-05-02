using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using TrimDB.Core.SkipList;

namespace TrimDB.Benchmarks
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //var sl = new SkipListInsert();
            //sl.GlobalSetup();
            //await sl.MultiThreaded();

            var summary = BenchmarkRunner.Run<SkipListInsert>();

            
        }
    }
}
