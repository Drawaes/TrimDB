using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage.Blocks
{
    public readonly struct FileIdentifier : IEquatable<FileIdentifier>
    {
        public int Level { get; }
        public int FileId { get; }

        public FileIdentifier(int level, int fileId)
        {
            Level = level;
            FileId = fileId;
        }

        public override bool Equals(object? obj)
        {
            return obj is FileIdentifier identifier && Equals(identifier);
        }

        public bool Equals(FileIdentifier other)
        {
            return Level == other.Level &&
                   FileId == other.FileId;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Level, FileId);
        }

        public static bool operator ==(FileIdentifier left, FileIdentifier right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(FileIdentifier left, FileIdentifier right)
        {
            return !(left == right);
        }
    }
}
