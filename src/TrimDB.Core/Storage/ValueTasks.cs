using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public static class ValueTasks
    {
        public static ValueTask<(SearchResult result, ReadOnlyMemory<byte> value)> CreateResult(SearchResult result, ReadOnlyMemory<byte> memory)
        {
            return new ValueTask<(SearchResult result, ReadOnlyMemory<byte> value)>((result, memory));
        }
    }
}
