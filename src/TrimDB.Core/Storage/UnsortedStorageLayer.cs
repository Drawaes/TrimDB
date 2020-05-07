using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class UnsortedStorageLayer : StorageLayer
    {
        private int _level;
        private int _maxFileIndex = -1;
        private string _databaseFolder;
        private TableFile[] _tableFiles;
        private int[] _tableFileIndices;

        public override int MaxFilesAtLayer => 6;

        public override int MaxSizeAtLayer => 1024 * 1024 * 1024;

        public override int NumberOfTables => _tableFiles.Length;

        public UnsortedStorageLayer(int level, string databaseFolder)
        {
            _databaseFolder = databaseFolder;
            _level = level;

            var levelFiles = System.IO.Directory.GetFiles(_databaseFolder, "Level*_*.trim");

            _tableFiles = new TableFile[levelFiles.Length];
            _tableFileIndices = new int[levelFiles.Length];

            if (_tableFiles.Length > 0)
            {
                for (var i = 0; i < _tableFiles.Length; i++)
                {
                    var table = new TableFile(levelFiles[i]);
                    if (table.Level != level)
                    {
                        throw new InvalidOperationException();
                    }
                    _tableFileIndices[i] = table.Index;
                    _tableFiles[i] = table;
                }

                Array.Sort(_tableFileIndices, _tableFiles);
                _maxFileIndex = _tableFileIndices[^1];
            }
        }

        public void AddTableFile(TableFile tableFile)
        {
            var newArray = new TableFile[_tableFiles.Length + 1];
            Array.Copy(_tableFiles, newArray, _tableFiles.Length);
            newArray[^1] = tableFile;

            Interlocked.Exchange(ref _tableFiles, newArray);
        }

        public string GetNextFileName()
        {
            var nextFileIndex = Interlocked.Increment(ref _maxFileIndex);
            return System.IO.Path.Combine(_databaseFolder, $"Level{_level}_{nextFileIndex}.trim");
        }

        public override async ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            var tfs = _tableFiles;
            foreach (var tf in tfs)
            {
                var result = await tf.GetAsync(key, hash);
                if (result.Result == SearchResult.Deleted || result.Result == SearchResult.Found)
                {
                    return result;
                }
            }

            return new SearchResultValue() { Result = SearchResult.NotFound };
        }
    }
}
