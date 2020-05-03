using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace TrimDB.Core.Interop.Windows
{
    internal static class Memory
    {
        [DllImport(Libraries.Kernel32, SetLastError = true)]
        internal extern static IntPtr VirtualAlloc(IntPtr lpAddress, UIntPtr dwSize, AllocationType flAllocationType, Protection flProtect);

        [DllImport(Libraries.Kernel32, SetLastError = true)]
        internal extern static bool VirtualFree(IntPtr lpAddress, UIntPtr dwSize, FreeType dwFreeType);

        [Flags]
        internal enum Protection
        {
            PAGE_READWRITE = 0x04,
        }

        [Flags]
        internal enum AllocationType
        {
            MEM_COMMIT = 0x00001000,
            MEM_RESERVE = 0x00002000
        }

        [Flags]
        internal enum FreeType
        {
            MEM_DECOMMIT = 0x00004000,
            MEM_RELEASE = 0x00008000,
        }
    }
}
