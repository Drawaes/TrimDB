using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class TableFile
    {
        public TableFile(string fileName)
        {

        }

        internal ValueTask<(SearchResult result, Memory<byte> value)> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            throw new NotImplementedException();
        }
    }
}
