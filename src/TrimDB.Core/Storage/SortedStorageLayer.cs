using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class SortedStorageLayer : StorageLayer
    {
        private readonly int _level;
        private string _databaseFolder;
        private TableFile[] _tableFiles;

        public SortedStorageLayer(int level, string databaseFolder)
        {
            _databaseFolder = databaseFolder;
            _level = level;

            var levelFiles = System.IO.Directory.GetFiles(_databaseFolder, "Level*_*.trim");

            _tableFiles = new TableFile[levelFiles.Length];

            for (var i = 0; i < _tableFiles.Length; i++)
            {
                var table = new TableFile(levelFiles[i]);
                if (table.Level != level)
                {
                    throw new InvalidOperationException();
                }
                _tableFiles[i] = table;
            }
        }

        public override async ValueTask<(SearchResult result, Memory<byte> value)> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            return (SearchResult.NotFound, new Memory<byte>());
        }
    }
}
