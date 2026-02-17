using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using TrimDB.Core.Storage.Layers;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class FileManagementFacts : IDisposable
    {
        private readonly string _folder;

        public FileManagementFacts()
        {
            _folder = Path.Combine(Path.GetTempPath(), "TrimDB_FileMgmt_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_folder);
        }

        public void Dispose()
        {
            if (Directory.Exists(_folder))
            {
                Directory.Delete(_folder, true);
            }
        }

        /// <summary>
        /// Test #88: FileIdentifier parses level and index from filename.
        /// TableFile constructor does the parsing: "Level2_15.trim" -> Level=2, FileId=15.
        /// We can't construct a TableFile without a real file, but we can verify
        /// FileIdentifier directly and verify the parsing logic by creating a dummy file.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void FileIdentifierParsesLevelAndIndex()
        {
            // FileIdentifier itself is just a struct with Level and FileId
            var fid = new FileIdentifier(2, 15);
            Assert.Equal(2, fid.Level);
            Assert.Equal(15, fid.FileId);

            // Verify the parsing done in TableFile constructor by creating a dummy file
            // with the right name pattern and checking the FileId property.
            var filePath = Path.Combine(_folder, "Level3_42.trim");
            File.WriteAllBytes(filePath, Array.Empty<byte>());

            using var cache = new MMapBlockCache();
            var tableFile = new TableFile(filePath, cache);

            Assert.Equal(3, tableFile.FileId.Level);
            Assert.Equal(42, tableFile.FileId.FileId);
        }

        /// <summary>
        /// Test #89: GetNextFileName returns incrementing, unique filenames.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void GetNextFileNameIncrements()
        {
            using var cache = new MMapBlockCache();
            var layer = new UnsortedStorageLayer(1, _folder, cache);

            var name1 = layer.GetNextFileName();
            var name2 = layer.GetNextFileName();
            var name3 = layer.GetNextFileName();

            // All three must be different
            Assert.NotEqual(name1, name2);
            Assert.NotEqual(name2, name3);
            Assert.NotEqual(name1, name3);

            // They should all be in the right directory and follow the naming pattern
            Assert.StartsWith(Path.Combine(_folder, "Level1_"), name1);
            Assert.EndsWith(".trim", name1);
            Assert.StartsWith(Path.Combine(_folder, "Level1_"), name2);
            Assert.StartsWith(Path.Combine(_folder, "Level1_"), name3);
        }

        /// <summary>
        /// Test #90: 10 threads each call GetNextFileName(). All 10 filenames are unique.
        /// Interlocked.Increment on _maxFileIndex guarantees this.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void ConcurrentGetNextFileNameNeverCollides()
        {
            using var cache = new MMapBlockCache();
            var layer = new UnsortedStorageLayer(1, _folder, cache);

            const int threadCount = 10;
            var names = new ConcurrentBag<string>();

            Parallel.For(0, threadCount, _ =>
            {
                names.Add(layer.GetNextFileName());
            });

            // All names must be unique
            var uniqueNames = new System.Collections.Generic.HashSet<string>(names);
            Assert.Equal(threadCount, uniqueNames.Count);
        }
    }
}
