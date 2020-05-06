using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.InMemory
{
    public interface IMemoryItem
    {
        ReadOnlySpan<byte> Key { get; }
        ReadOnlySpan<byte> Value { get; }
        bool IsDeleted { get; }
    }
}
