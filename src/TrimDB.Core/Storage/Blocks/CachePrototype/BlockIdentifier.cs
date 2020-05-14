using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage.Blocks.CachePrototype
{
    internal struct BlockIdentifier
    {
        public ushort LevelId { get; set; }
        public ushort FileId { get; set; }
        public uint BlockId { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is BlockIdentifier identifier &&
                   LevelId == identifier.LevelId &&
                   FileId == identifier.FileId &&
                   BlockId == identifier.BlockId;
        }

        public override int GetHashCode() => HashCode.Combine(LevelId, FileId, BlockId);

        public override string? ToString() => $"{LevelId}-{FileId}-{BlockId}";
    }
}
