using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Blocks;

namespace TrimDB.Core.Storage.Layers
{
    public class UnsortedStorageLayer : StorageLayer
    {
        private readonly int _maxFilesAtLayer;

        public override int MaxFilesAtLayer => _maxFilesAtLayer;

        public override int NumberOfTables => _tableFiles.Length;

        public UnsortedStorageLayer(int level, string databaseFolder, BlockCache blockCache, int maxFiles = 6)
            : base(databaseFolder, level, blockCache, 0)
        {
            _maxFilesAtLayer = maxFiles;
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

        public override async ValueTask<ValueLease> GetWithLeaseAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            var tfs = _tableFiles;

            for (var i = tfs.Length - 1; i >= 0; i--)
            {
                var lease = await tfs[i].GetWithLeaseAsync(key, hash);
                if (lease.IsFound || lease.IsDeleted)
                {
                    return lease;
                }
            }

            return ValueLease.Empty;
        }
    }
}
