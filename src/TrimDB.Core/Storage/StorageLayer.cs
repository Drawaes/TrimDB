using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public abstract class StorageLayer
    {
        public abstract int MaxFilesAtLayer { get;}
        public abstract int MaxSizeAtLayer { get; }
        public abstract int NumberOfTables { get;}

        public abstract ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash);
    }
}
