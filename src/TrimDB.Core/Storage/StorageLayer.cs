using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public abstract class StorageLayer
    {
        private string _databaseFolder;
        private int _level;
        protected TableFile[] _tableFiles;
        protected int[] _tableFileIndices;
        private int _maxFileIndex = -1;
        private BlockCache.BlockCache _blockCache;


        public StorageLayer(string databaseFolder, int level, BlockCache.BlockCache blockCache)
        {
            _databaseFolder = databaseFolder;
            _level = level;

            var levelFiles = System.IO.Directory.GetFiles(_databaseFolder, "Level???_*.trim");

            _tableFiles = new TableFile[levelFiles.Length];
            _tableFileIndices = new int[levelFiles.Length];

            if (_tableFiles.Length > 0)
            {
                for (var i = 0; i < _tableFiles.Length; i++)
                {
                    var table = new TableFile(levelFiles[i], _blockCache);
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

        public abstract int MaxFilesAtLayer { get; }
        public abstract int MaxFileSize { get; }
        public abstract int NumberOfTables { get; }
        public int Level => _level;

        internal TableFile[] GetTables() => _tableFiles;

        public abstract ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash);

        public string GetNextFileName()
        {
            var nextFileIndex = Interlocked.Increment(ref _maxFileIndex);
            return System.IO.Path.Combine(_databaseFolder, $"Level{_level}_{nextFileIndex}.trim");
        }

        public void AddTableFile(TableFile tableFile)
        {
            while (true)
            {
                var tfs = _tableFiles;
                var newArray = new TableFile[tfs.Length + 1];
                Array.Copy(tfs, newArray, tfs.Length);
                newArray[^1] = tableFile;

                if (Interlocked.CompareExchange(ref _tableFiles, newArray, tfs) == tfs)
                {
                    return;
                }
            }
        }

        internal void RemoveTable(TableFile table)
        {
            while (true)
            {
                var tfs = _tableFiles;
                var newArray = new TableFile[_tableFiles.Length - 1];
                var tfCounter = 0;
                for (var i = 0; i < newArray.Length; i++)
                {
                    if (tfs[tfCounter] == table)
                    {
                        tfCounter++;
                    }
                    newArray[i] = tfs[tfCounter];
                    tfCounter++;
                }
                if (Interlocked.CompareExchange(ref _tableFiles, newArray, tfs) == tfs)
                {
                    return;
                }
            }
        }

        internal void AddAndRemoveTableFiles(List<TableFile> newTableFiles, List<TableFile> overlapped)
        {
            while (true)
            {
                var tfs = _tableFiles;
                var newLength = newTableFiles.Count + tfs.Length - (overlapped.Count - 1);

                var newTable = new TableFile[newLength];
                var tfCounter = 0;
                for (var i = 0; i < newTable.Length - newTableFiles.Count; i++)
                {
                    while (overlapped.Contains(tfs[tfCounter]))
                    {
                        tfCounter++;
                    }
                    newTable[i] = tfs[tfCounter];
                    tfCounter++;
                }

                newTableFiles.CopyTo(newTable, newTable.Length - newTableFiles.Count);
                if (Interlocked.CompareExchange(ref _tableFiles, newTable, tfs) == tfs)
                {
                    return;
                }
            }

        }
    }
}
