using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage.MetaData
{
    public enum TableOfContentsEntryType : short
    {
        BlockOffsets = 1,
        Statistics = 2,
        Filter = 3
    }
}
