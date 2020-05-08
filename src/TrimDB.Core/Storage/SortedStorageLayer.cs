using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class SortedStorageLayer : StorageLayer
    {
        public SortedStorageLayer(int level, string databaseFolder)
            : base(databaseFolder, level)
        {
        }

        public override int MaxFilesAtLayer => (int)(Math.Pow(10, Level) * 2);

        public override int MaxSizeAtLayer => (int)(Math.Pow(10, Level - 1) * 1024 * 1024 * 8);

        public override int NumberOfTables => _tableFiles.Length;

        public override ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            return SearchResultValue.CreateValueTask(SearchResult.NotFound);
        }
    }
}
