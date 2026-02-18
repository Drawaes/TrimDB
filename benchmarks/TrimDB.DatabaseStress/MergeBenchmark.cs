using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using TrimDB.Core;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Layers;

namespace TrimDB.DatabaseStress
{
    [MemoryDiagnoser]
    public class MergeBenchmark
    {
        private BlockCache _cache;
        private TableFile[] _tableFiles;
        private const string _outputFolder = "z:\\stressoutput";
        private StorageLayer _storageLayer;

        [GlobalSetup]
        public void GlobalSetup()
        {
            if (Directory.Exists(_outputFolder))
            {
                Directory.Delete(_outputFolder, true);
            }
            Directory.CreateDirectory(_outputFolder);

            var dbFolder = "D:\\stress";
            var dbOptions = new TrimDatabaseOptions();


            _cache = dbOptions.BlockCache();
            var filenames = Directory.GetFiles(dbFolder);
            _tableFiles = new TableFile[filenames.Length];

            _storageLayer = new SortedStorageLayer(5, _outputFolder, _cache, 10 * 1024 * 1024, 100);

            for (var i = 0; i < _tableFiles.Length; i++)
            {
                var tf = new TableFile(filenames[i], _cache);
                tf.LoadAsync().Wait();
                _tableFiles[i] = tf;
            }
        }

        [Benchmark(Baseline = true)]
        public async Task MergeFiles()
        {
            var merger = new TableFileMerger(_tableFiles.Select(f => f.GetAsyncEnumerator()).ToArray());
            var mWriter = new TableFileMergeWriter(_storageLayer, _cache, loadNewFiles: false);

            await mWriter.WriteFromMerger(merger);
        }
    }
}
