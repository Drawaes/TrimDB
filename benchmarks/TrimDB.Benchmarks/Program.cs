using BenchmarkDotNet.Running;

namespace TrimDB.Benchmarks
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            //var sl = new SkipListInsert();
            //sl.GlobalSetup();
            //await sl.MultiThreaded();

            var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
