using System;
using System.IO;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class DatabaseTestBase : IAsyncLifetime
    {
        protected readonly string _folder;
        protected TrimDatabase _db;

        public DatabaseTestBase()
        {
            _folder = Path.Combine(Path.GetTempPath(), "TrimDB_" + Guid.NewGuid().ToString("N"));
        }

        public virtual async Task InitializeAsync()
        {
            var options = CreateOptions();
            _db = new TrimDatabase(options);
            await _db.LoadAsync();
        }

        protected virtual TrimDatabaseOptions CreateOptions()
        {
            return new TrimDatabaseOptions
            {
                DatabaseFolder = _folder,
                BlockCache = () => new MMapBlockCache(),
                DisableMerging = true,
                DisableWAL = true,
                DisableManifest = true,
                MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 1000, 25))
            };
        }

        public virtual async Task DisposeAsync()
        {
            if (_db != null)
            {
                try { await _db.DisposeAsync(); } catch { }
            }
            try
            {
                if (Directory.Exists(_folder))
                    Directory.Delete(_folder, true);
            }
            catch { }
        }
    }
}
