using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Blocks;

namespace TrimDB.Core.Storage.Layers
{
    public abstract class StorageLayer
    {
        private readonly string _databaseFolder;
        protected readonly int _level;
        protected TableFile[] _tableFiles;
        protected int[] _tableFileIndices;
        private int _maxFileIndex = -1;
        private readonly BlockCache _blockCache;

        public StorageLayer(string databaseFolder, int level, BlockCache blockCache, int targetFileSize)
        {
            MaxFileSize = targetFileSize;
            _databaseFolder = databaseFolder;
            _level = level;
            _blockCache = blockCache;

            var levelFiles = System.IO.Directory.GetFiles(_databaseFolder, $"Level{level}_*.trim");

            _tableFiles = new TableFile[levelFiles.Length];
            _tableFileIndices = new int[levelFiles.Length];

            if (_tableFiles.Length > 0)
            {
                for (var i = 0; i < _tableFiles.Length; i++)
                {
                    var table = new TableFile(levelFiles[i], _blockCache);
                    if (table.FileId.Level != level)
                    {
                        throw new InvalidOperationException();
                    }
                    _tableFileIndices[i] = table.FileId.FileId;
                    _tableFiles[i] = table;
                }

                Array.Sort(_tableFileIndices, _tableFiles);
                _maxFileIndex = _tableFileIndices[^1];
            }
        }

        public async Task LoadLayer()
        {
            foreach(var tf in _tableFiles)
            {
                await tf.LoadAsync();
            }
        }

        public abstract int MaxFilesAtLayer { get; }
        public int MaxFileSize { get; protected set; }
        public abstract int NumberOfTables { get; }
        public int Level => _level;

        public TableFile[] GetTables() => _tableFiles;

        public abstract ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash);

        internal int[] GetFileIndices()
        {
            var tfs = Volatile.Read(ref _tableFiles);
            var result = new int[tfs.Length];
            for (var i = 0; i < tfs.Length; i++)
                result[i] = tfs[i].FileId.FileId;
            return result;
        }

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
