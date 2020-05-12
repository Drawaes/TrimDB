using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace TrimDB.Core.Interop.Windows
{
    internal static class CompletionPorts
    {
        [DllImport(Libraries.Kernel32, SetLastError = true)]
        internal static extern CompletionPortSafeHandle CreateIoCompletionPort(IntPtr fileHandle, IntPtr existingCompletionPort, UIntPtr completionKey, uint NumberOfConcurrentThreads);

        [DllImport(Libraries.Kernel32, SetLastError = false)]
        internal static extern bool CloseHandle(IntPtr handle);

        [DllImport(Libraries.Kernel32, SetLastError = false)]
        internal static extern bool GetQueuedCompletionStatus(CompletionPortSafeHandle completionPort,out uint lpNumberOfBytesTransferred, out UIntPtr lpCompletionKey, out IntPtr lpOverlapped, int dwMilliseconds);

        public class CompletionPortSafeHandle : SafeHandle
        {
            public CompletionPortSafeHandle() : base(IntPtr.Zero, ownsHandle: true)
            {
            }

            public override bool IsInvalid => handle == IntPtr.Zero;

            protected override bool ReleaseHandle()
            {
                return CloseHandle(handle);
            }
        }
    }
}
