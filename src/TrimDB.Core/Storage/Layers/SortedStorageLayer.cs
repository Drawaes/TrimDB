using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Blocks;

namespace TrimDB.Core.Storage.Layers
{
    public class SortedStorageLayer : StorageLayer
    {
        public SortedStorageLayer(int level, string databaseFolder, BlockCache blockCache)
            : base(databaseFolder, level, blockCache)
        {
        }

        public override int MaxFilesAtLayer => (int)(Math.Pow(10, Level) * 2);

        public override int MaxFileSize => 1024 * 1024 * 8;

        public override int NumberOfTables => _tableFiles.Length;

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
