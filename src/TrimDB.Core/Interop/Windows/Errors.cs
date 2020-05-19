using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Interop.Windows
{
    internal static class Errors
    {
        internal enum NTError : int
        {
            ERROR_SUCCESS = 0,
            ERROR_INVALID_FUNCTION = 1,
            ERROR_FILE_NOT_FOUND = 2,
            ERROR_PATH_NOT_FOUND = 3,
            ERROR_TOO_MANY_OPEN_FILES = 4,
            ERROR_ACCESS_DENIED = 5,
            ERROR_INVALID_HANDLE = 6,
            ERROR_ARENA_TRASHED = 7,
            ERROR_NOT_ENOUGH_MEMORY = 8,
            ERROR_INVALID_BLOCK = 9,
            ERROR_BAD_ENVIRONMENT = 10,
            ERROR_BAD_FORMAT = 11,
            ERROR_INVALID_ACCESS = 12,
            ERROR_INVALID_DATA = 13,
            ERROR_OUTOFMEMORY = 14,
            ERROR_INVALID_DRIVE = 15,
            ERROR_CURRENT_DIRECTORY = 16,
            ERROR_NOT_SAME_DEVICE = 17,
            ERROR_NO_MORE_FILES = 18,
            ERROR_WRITE_PROTECT = 19,
            ERROR_BAD_UNIT = 20,
            ERROR_NOT_READY = 21,
            ERROR_BAD_COMMAND = 22,
            ERROR_CRC = 23,
            ERROR_BAD_LENGTH = 24,
            ERROR_SEEK = 25,
            ERROR_NOT_DOS_DISK = 26,
            ERROR_SECTOR_NOT_FOUND = 27,
            ERROR_OUT_OF_PAPER = 28,
            ERROR_WRITE_FAULT = 29,
            ERROR_READ_FAULT = 30,
            ERROR_GEN_FAILURE = 31,
            ERROR_SHARING_VIOLATION = 32,
            ERROR_LOCK_VIOLATION = 33,
            ERROR_WRONG_DISK = 34,
            ERROR_SHARING_BUFFER_EXCEEDED = 36,
            ERROR_HANDLE_EOF = 38,
            ERROR_HANDLE_DISK_FULL = 39,
            ERROR_NOT_SUPPORTED = 50,
            ERROR_REM_NOT_LIST = 51,
            ERROR_DUP_NAME = 52,
            ERROR_BAD_NETPATH = 53,
            ERROR_NETWORK_BUSY = 54,
            ERROR_DEV_NOT_EXIST = 55,
            ERROR_TOO_MANY_CMDS = 56,
            ERROR_ADAP_HDW_ERR = 57,
            ERROR_BAD_NET_RESP = 58,
            ERROR_UNEXP_NET_ERR = 59,
            ERROR_BAD_REM_ADAP = 60,
            ERROR_PRINTQ_FULL = 61,
            ERROR_NO_SPOOL_SPACE = 62,
            ERROR_PRINT_CANCELLED = 63,
            ERROR_NETNAME_DELETED = 64,
            ERROR_NETWORK_ACCESS_DENIED = 65,
            ERROR_BAD_DEV_TYPE = 66,
            ERROR_BAD_NET_NAME = 67,
            ERROR_TOO_MANY_NAMES = 68,
            ERROR_TOO_MANY_SESS = 69,
            ERROR_SHARING_PAUSED = 70,
            ERROR_REQ_NOT_ACCEP = 71,
            ERROR_REDIR_PAUSED = 72,

            ERROR_ABANDONED_WAIT_0 = 735,

            ERROR_IO_PENDING = 997,
        }
    }
}
