using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class UnsortedStorageLayer : StorageLayer
    {
        private readonly List<TableFile> _tableFile = new List<TableFile>();

        public override async ValueTask<(SearchResult result, Memory<byte> value)> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            foreach (var tf in _tableFile)
            {
                var (result, value) = await tf.GetAsync(key, hash);
                if (result == SearchResult.Deleted || result == SearchResult.Found)
                {
                    return (result, value);
                }
            }

            return (SearchResult.NotFound, default);
        }
    }
}
