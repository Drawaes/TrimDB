using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace TrimDB.Core.Storage
{
    [StructLayout(LayoutKind.Explicit)]
    public struct TocEntry
    {
        [FieldOffset(0)]
        public long Offset;
        [FieldOffset(8)]
        public int Length;
        [FieldOffset(12)]
        public TocEntryType EntryType;
    }

    public enum TocEntryType : short
    {
        BlockOffsets = 1,
        Statistics = 2,
        Filter = 3 
    }
}
