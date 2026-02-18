using System;
using System.Buffers;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using TrimDB.Core;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;

namespace TrimDB.Benchmarks
{
    [Config(typeof(BlockSearchBenchConfig))]
    public class BlockSearchBench
    {
        private const int EntryCount = 100;
        private const int BlockSize = 4096;

        // In-memory block data
        private byte[] _blockData = null!;
        private FixedMemoryOwner _blockOwner = null!;

        // Keys for lookup
        private byte[] _middleKey = null!;
        private byte[] _firstKey = null!;
        private byte[] _missingKey = null!;

        // File-backed SSTable
        private string _tempFolder = null!;
        private TableFile _tableFile = null!;
        private MMapBlockCache _blockCache = null!;
        private byte[] _tableMiddleKey = null!;

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            // ---- In-memory block ----
            _blockData = new byte[BlockSize];
            var builder = new SlottedBlockBuilder(_blockData);
            var keys = new byte[EntryCount][];

            for (int i = 0; i < EntryCount; i++)
            {
                keys[i] = Encoding.UTF8.GetBytes($"block_key_{i:D6}");
            }

            // Sort keys lexicographically (they're already sorted due to formatting, but be safe)
            Array.Sort(keys, (a, b) => a.AsSpan().SequenceCompareTo(b));

            foreach (var key in keys)
            {
                var value = Encoding.UTF8.GetBytes($"val_{key.Length}");
                if (!builder.TryAdd(key, value, isDeleted: false))
                    break;
            }
            builder.Finish();

            _blockOwner = new FixedMemoryOwner(_blockData);
            _firstKey = keys[0];
            _middleKey = keys[EntryCount / 2];
            _missingKey = Encoding.UTF8.GetBytes("block_key_zzzzzz");

            // ---- File-backed SSTable ----
            _tempFolder = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "trimdb_block_bench_" + Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(_tempFolder);

            var allocator = new ArrayBasedAllocator32(16 * 1024 * 1024, 25);
            var skipList = new SkipList32(allocator);

            for (int i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"sst_key_{i:D6}");
                var value = Encoding.UTF8.GetBytes($"sst_value_{i:D6}");
                skipList.Put(key, value);
            }

            var fileName = System.IO.Path.Combine(_tempFolder, "Level1_1.trim");
            var writer = new TableFileWriter(fileName);
            await writer.SaveMemoryTable(skipList);

            _blockCache = new MMapBlockCache();
            _tableFile = new TableFile(fileName, _blockCache);
            await _tableFile.LoadAsync();

            _tableMiddleKey = Encoding.UTF8.GetBytes("sst_key_000500");
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _blockCache?.Dispose();
            try { System.IO.Directory.Delete(_tempFolder, true); } catch { }
        }

        // --- BlockReader benchmarks (class-based, IDisposable) ---

        [Benchmark(Baseline = true)]
        public BlockReader.KeySearchResult BlockReader_FindMiddle()
        {
            using var reader = new BlockReader(new FixedMemoryOwner(_blockData));
            return reader.TryFindKey(_middleKey);
        }

        [Benchmark]
        public BlockReader.KeySearchResult BlockReader_FindFirst()
        {
            using var reader = new BlockReader(new FixedMemoryOwner(_blockData));
            return reader.TryFindKey(_firstKey);
        }

        [Benchmark]
        public BlockReader.KeySearchResult BlockReader_FindMissing()
        {
            using var reader = new BlockReader(new FixedMemoryOwner(_blockData));
            return reader.TryFindKey(_missingKey);
        }

        // --- BlockView benchmarks (ref struct, zero-alloc) ---

        [Benchmark]
        public BlockReader.KeySearchResult BlockView_FindMiddle()
        {
            var view = new BlockView(_blockData);
            return view.TryFindKey(_middleKey);
        }

        [Benchmark]
        public BlockReader.KeySearchResult BlockView_FindFirst()
        {
            var view = new BlockView(_blockData);
            return view.TryFindKey(_firstKey);
        }

        [Benchmark]
        public int BlockView_IterateAll()
        {
            var view = new BlockView(_blockData);
            int count = 0;
            while (view.TryGetNextKey(out _))
            {
                count++;
            }
            return count;
        }

        // --- Full SSTable path ---

        [Benchmark]
        public async Task<ReadOnlyMemory<byte>> TableFile_FindKey()
        {
            var hash = new Core.Hashing.MurmurHash3().ComputeHash64(_tableMiddleKey);
            var result = await _tableFile.GetAsync(_tableMiddleKey, hash);
            return result.Value;
        }
    }

    /// <summary>
    /// Wraps a byte[] as IMemoryOwner without disposing the underlying array.
    /// Used for benchmarking BlockReader without heap allocation noise from pooling.
    /// </summary>
    internal sealed class FixedMemoryOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _data;

        public FixedMemoryOwner(byte[] data) => _data = data;
        public Memory<byte> Memory => _data;
        public void Dispose() { } // intentional no-op
    }

    /// <summary>
    /// Config with deeper disassembly (depth 3) for block binary search hot path.
    /// </summary>
    public class BlockSearchBenchConfig : ManualConfig
    {
        public BlockSearchBenchConfig()
        {
            AddDiagnoser(MemoryDiagnoser.Default);
            AddDiagnoser(new DisassemblyDiagnoser(new DisassemblyDiagnoserConfig(
                maxDepth: 3,
                syntax: DisassemblySyntax.Masm,
                exportGithubMarkdown: true)));
            AddExporter(HtmlExporter.Default);
            AddExporter(MarkdownExporter.GitHub);
        }
    }
}
