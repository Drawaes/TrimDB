using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Blocks;

namespace TrimDB.Core.Storage.Layers
{
    public class UnsortedStorageLayer : StorageLayer
    {
        public override int MaxFilesAtLayer => 6;

        public override int NumberOfTables => _tableFiles.Length;

        public UnsortedStorageLayer(int level, string databaseFolder, BlockCache blockCache)
            : base(databaseFolder, level, blockCache, 0)
        {

        }

        public override async ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            var tfs = _tableFiles;

            // Search the most recent (largest index) first
            for (var i = tfs.Length - 1; i >= 0; i--)
            {
                var tf = tfs[i];
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
