using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace TrimDB.Core.Interop.Windows
{
    internal static class Files
    {
        [DllImport(Libraries.Kernel32, SetLastError = true)]
        internal unsafe static extern bool ReadFile(SafeFileHandle hFile, void* lpBuffer, uint nNumberOfBytesToRead, out uint lpNumberOfBytesRead, IntPtr overlapped);

        [DllImport(Libraries.Kernel32, CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern SafeFileHandle CreateFileW([In] string lpFileName, FileAccess dwDesiredAccess, FileShare dwShareMode, IntPtr lpSecurityAttributes, FileMode dwCreationDisposition, FileFlag dwFlagsAndAttributes, IntPtr hTemplateFile);

        [Flags]
        internal enum FileAccess : uint
        {
            GENERIC_READ = 0x80000000,
            GENERIC_WRITE = 0x40000000,
        }

        [Flags]
        internal enum FileFlag : uint
        {
            NO_BUFFERING = 0x20000000,
            OVERLAPPED = 0x40000000
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct OverlappedStruct
        {
            public IntPtr Internal;
            public IntPtr InternalHigh;
            public uint Offset;
            public uint OffsetHigh;
            public IntPtr Pointer;
            public IntPtr hEvent;
            public int LevelId;
            public int FileId;
            public int BlockId;
            public int Reserved;
        }
    }
}
