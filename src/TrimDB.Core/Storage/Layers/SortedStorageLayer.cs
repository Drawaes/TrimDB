using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Blocks;

namespace TrimDB.Core.Storage.Layers
{
    public class SortedStorageLayer : StorageLayer
    {
        public SortedStorageLayer(int level, string databaseFolder, BlockCache blockCache, int targetFileSize, int maxFiles)
            : base(databaseFolder, level, blockCache, targetFileSize)
        {
            MaxFilesAtLayer = maxFiles;
        }

        public override int MaxFilesAtLayer { get; }
                
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

        public IEnumerable<(ReadOnlyMemory<byte> firstKey, ReadOnlyMemory<byte> lastKey)> GetFirstAndLastKeys()
        {
            var tfs = _tableFiles;
            foreach (var tf in tfs)
            {
                yield return (tf.FirstKey, tf.LastKey);
            }
        }
    }
}
