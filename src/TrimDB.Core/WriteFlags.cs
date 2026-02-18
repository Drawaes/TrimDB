using System;

namespace TrimDB.Core
{
    [Flags]
    public enum WriteFlags
    {
        None = 0,
        LowPriority = 1,
        DisableWAL = 2,
        LowPriorityAndNoWAL = 3
    }
}
