using System;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;

namespace TrimDB.Core
{
    public class TrimDatabaseOptions
    {
        public Func<MemoryTable> MemoryTable { get; set; } = () => new SkipList32(new ArrayBasedAllocator32(64 * 1024 * 1024, 25));
        public Func<BlockCache> BlockCache { get; set; } = () => new MMapBlockCache();
        public int Levels { get; set; } = 5;
        public string DatabaseFolder { get; set; } = null!;

        // public int[] FilesPerLevel { get; set; }
        public double LevelIncreaseFactor { get; set; } = 4;
        public int FirstLevelMaxFileCount { get; set; } = 2;

        public int FileSize { get; set; } = 1024 * 1024 * 16;
        public bool OpenReadOnly { get; set; }

        public bool DisableMerging { get; set; }

        public bool DisableWAL { get; set; }
        public bool WalWaitForFlush { get; set; } = true;

        public bool DisableManifest { get; set; }

        public int MaxL0Files { get; set; } = 6;
        public int MaxMemtableFlushBacklog { get; set; } = 2;

        public int WalChannelCapacity { get; set; } = 4096;
    }
}
