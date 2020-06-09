using System;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.CachePrototype;

namespace TrimDB.Core
{
    public class TrimDatabaseOptions
    {
        public Func<MemoryTable> MemoryTable { get; set; } = () => new SkipList32(new NativeAllocator32(10 * 1024 * 1024, 25));
        public Func<BlockCache> BlockCache { get; set; } = () => new ProtoSharded(2_560);
        public int Levels { get; set; } = 5;
        public string DatabaseFolder { get; set; }

        // public int[] FilesPerLevel { get; set; }
        public double LevelIncreaseFactor { get; set; } = 4;
        public int FirstLevelMaxFileCount { get; set; } = 2;

        public int FileSize { get; set; } = 1024 * 1024 * 16;
        public bool OpenReadOnly { get; set; }

    }
}
