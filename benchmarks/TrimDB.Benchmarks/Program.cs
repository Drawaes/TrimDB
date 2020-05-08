using System;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using TrimDB.Core;
using TrimDB.Core.InMemory.SkipList32;

namespace TrimDB.Benchmarks
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
