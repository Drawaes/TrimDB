using System;

namespace TrimDB.Core
{
    [Flags]
    public enum ReadFlags
    {
        None = 0,
        HintCacheMiss = 1,
        HintReadAhead = 2,
        HintReadAhead2 = 4,
        HintReadAhead3 = 8,
        SkipDuplicateRead = 16
    }
}
