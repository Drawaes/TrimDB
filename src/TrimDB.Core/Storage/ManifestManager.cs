using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Hashing;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public sealed class ManifestData
    {
        private readonly Dictionary<int, List<int>> _filesByLevel = new();

        public void AddFile(int level, int fileIndex)
        {
            if (!_filesByLevel.TryGetValue(level, out var list))
            {
                list = new List<int>();
                _filesByLevel[level] = list;
            }
            list.Add(fileIndex);
        }

        public void RemoveFile(int level, int fileIndex)
        {
            if (_filesByLevel.TryGetValue(level, out var list))
            {
                list.Remove(fileIndex);
            }
        }

        public IReadOnlyList<int> GetFiles(int level)
        {
            if (_filesByLevel.TryGetValue(level, out var list))
                return list;
            return Array.Empty<int>();
        }

        public IEnumerable<int> Levels => _filesByLevel.Keys;
    }

    public sealed class ManifestManager
    {
        private const uint Magic = 0x4D414E49; // "MANI"
        private const uint FormatVersion = 1;

        private readonly string _manifestPath;
        private readonly string _tempPath;

        public ManifestManager(string databaseFolder)
        {
            _manifestPath = Path.Combine(databaseFolder, "manifest.mf");
            _tempPath = _manifestPath + ".tmp";
        }

        public bool Exists => File.Exists(_manifestPath);

        public ManifestData? TryRead()
        {
            if (!File.Exists(_manifestPath))
                return null;

            var bytes = File.ReadAllBytes(_manifestPath);
            if (bytes.Length < 16) // magic + version + levelCount + crc
                throw new InvalidDataException("Manifest file is too small");

            // Verify CRC (last 4 bytes)
            var dataLength = bytes.Length - 4;
            var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataLength));
            var actualCrc = Crc32.HashToUInt32(bytes.AsSpan(0, dataLength));
            if (expectedCrc != actualCrc)
                throw new InvalidDataException("Manifest CRC mismatch");

            var offset = 0;

            var magic = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset));
            offset += 4;
            if (magic != Magic)
                throw new InvalidDataException("Invalid manifest magic number");

            var version = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset));
            offset += 4;
            if (version != FormatVersion)
                throw new InvalidDataException($"Unsupported manifest version: {version}");

            var levelCount = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset));
            offset += 4;

            var data = new ManifestData();

            for (var i = 0; i < (int)levelCount; i++)
            {
                var level = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset));
                offset += 4;
                var fileCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset));
                offset += 4;

                for (var f = 0; f < fileCount; f++)
                {
                    var fileIndex = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset));
                    offset += 4;
                    data.AddFile(level, fileIndex);
                }
            }

            return data;
        }

        public async Task WriteAsync(ManifestData data)
        {
            // Calculate size
            var levels = new List<int>(data.Levels);
            var size = 12; // magic + version + levelCount
            foreach (var level in levels)
            {
                size += 8; // level + fileCount
                size += data.GetFiles(level).Count * 4; // fileIndices
            }
            size += 4; // CRC

            var buffer = new byte[size];
            var offset = 0;

            BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), Magic);
            offset += 4;
            BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), FormatVersion);
            offset += 4;
            BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), (uint)levels.Count);
            offset += 4;

            foreach (var level in levels)
            {
                var files = data.GetFiles(level);
                BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), (uint)level);
                offset += 4;
                BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), (uint)files.Count);
                offset += 4;

                foreach (var fileIndex in files)
                {
                    BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), (uint)fileIndex);
                    offset += 4;
                }
            }

            // CRC of all preceding bytes
            var crc = Crc32.HashToUInt32(buffer.AsSpan(0, offset));
            BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), crc);

            // Write to temp file, fsync, then atomic rename
            await using (var fs = new FileStream(_tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await fs.WriteAsync(buffer);
                await fs.FlushAsync();
                fs.Flush(flushToDisk: true);
            }

            File.Move(_tempPath, _manifestPath, overwrite: true);
        }

        public void CleanupTempFile()
        {
            if (File.Exists(_tempPath))
                File.Delete(_tempPath);
        }
    }
}
