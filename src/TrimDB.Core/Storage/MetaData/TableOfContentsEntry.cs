using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace TrimDB.Core.Storage.MetaData
{
    [StructLayout(LayoutKind.Explicit)]
    public struct TableOfContentsEntry
    {
        [FieldOffset(0)]
        public long Offset;
        [FieldOffset(8)]
        public int Length;
        [FieldOffset(12)]
        public TableOfContentsEntryType EntryType;
    }
}
