using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage
{
    internal static class FileConsts
    {
        internal const int PageSize = 4096;
        internal const uint MagicNumber = 0xDEADBEAF;
        internal const int Version = 1;
        internal const int TocEntryOffset = 12;
    }
}
