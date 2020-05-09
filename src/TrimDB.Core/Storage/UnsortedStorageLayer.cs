using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class UnsortedStorageLayer : StorageLayer
    {
        public override int MaxFilesAtLayer => 6;

        public override int MaxFileSize => 1024 * 1024 * 1024;

        public override int NumberOfTables => _tableFiles.Length;

        public UnsortedStorageLayer(int level, string databaseFolder, BlockCache.BlockCache blockCache)
            : base(databaseFolder, level, blockCache)
        {

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
