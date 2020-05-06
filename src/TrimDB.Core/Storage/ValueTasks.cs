using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public static class ValueTasks
    {
        public static ValueTask<(SearchResult result, Memory<byte> value)> CreateResult(SearchResult result, Memory<byte> memory)
        {
            return new ValueTask<(SearchResult result, Memory<byte> value)>((result, memory));
        }
    }
}
