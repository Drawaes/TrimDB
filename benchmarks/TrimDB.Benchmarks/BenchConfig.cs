using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;

namespace TrimDB.Benchmarks
{
    public class TrimBenchConfig : ManualConfig
    {
        public TrimBenchConfig()
        {
            AddDiagnoser(MemoryDiagnoser.Default);
            AddDiagnoser(new DisassemblyDiagnoser(new DisassemblyDiagnoserConfig(
                maxDepth: 2,
                syntax: DisassemblySyntax.Masm,
                exportGithubMarkdown: true)));
            AddExporter(HtmlExporter.Default);
            AddExporter(MarkdownExporter.GitHub);
        }
    }
}
