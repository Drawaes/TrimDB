using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class SortedStorageLayer : StorageLayer
    {
        private int _level;

        public SortedStorageLayer(int level)
        {
            _level = level;
        }

        public override async ValueTask<(SearchResult result, Memory<byte> value)> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            return (SearchResult.NotFound, new Memory<byte>());
        }
    }
}
