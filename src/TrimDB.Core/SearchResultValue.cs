using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TrimDB.Core
{
    public struct SearchResultValue
    {
        public SearchResult Result { get; set; }
        public ReadOnlyMemory<byte> Value { get; set; }

        public static ValueTask<SearchResultValue> CreateValueTask(SearchResult result, ReadOnlyMemory<byte> value = default)
        {
            return new ValueTask<SearchResultValue>(new SearchResultValue { Result = result, Value = value });
        }
    }
}
