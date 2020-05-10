using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Filters;

namespace TrimDB.Core.Storage.MetaData
{
    public class TableMetaData
    {
        private List<TableOfContentsEntry> _tocEntries = new List<TableOfContentsEntry>();
        private List<long> _blockEntries = new List<long>();
        private Filter _filter = new XorFilter();
        private ReadOnlyMemory<byte> _lastKey;
        private ReadOnlyMemory<byte> _firstKey;

        public int BlockCount => _blockEntries.Count;
        public ReadOnlyMemory<byte> FirstKey => _firstKey;
        public ReadOnlyMemory<byte> LastKey => _lastKey;
        public Filter Filter => _filter;

        public static async Task<TableMetaData> LoadFromFileAsync(string fileName)
        {
            await using var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
            var metaData = new TableMetaData();

            fs.Seek(-FileConsts.PageSize, SeekOrigin.End);
            using (var mem = MemoryPool<byte>.Shared.Rent(FileConsts.PageSize))
            {
                var footerMemory = mem.Memory.Slice(0, FileConsts.PageSize);
                await GetBlockFromFile(fs, footerMemory);
                ReadFooter(metaData, footerMemory);
            }

            using (var mem = await GetTOCMemory(fs, metaData, TableOfContentsEntryType.BlockOffsets))
            {
                ReadBlockIndex(metaData, mem.Memory);
            }

            using (var mem = await GetTOCMemory(fs, metaData, TableOfContentsEntryType.Filter))
            {
                ReadFilter(metaData, mem.Memory);
            }

            using (var mem = await GetTOCMemory(fs, metaData, TableOfContentsEntryType.Statistics))
            {
                ReadStatistics(metaData, mem.Memory);
            }
            return metaData;
        }

        private static void ReadFooter(TableMetaData metaData, ReadOnlyMemory<byte> memory)
        {
            var buffer = memory.Span;
            var magicNumber = BinaryPrimitives.ReadUInt32LittleEndian(buffer[^4..]);
            if (magicNumber != FileConsts.MagicNumber)
            {
                throw new InvalidOperationException("The magic number for the file was incorrect");
            }

            var tocSize = BinaryPrimitives.ReadInt32LittleEndian(buffer[^8..]);
            var version = BinaryPrimitives.ReadInt32LittleEndian(buffer[^12..]);

            buffer = buffer[^tocSize..^(sizeof(int) * 3)];

            while (buffer.Length > 0)
            {
                ref var ptr = ref MemoryMarshal.GetReference(buffer);
                metaData._tocEntries.Add(Unsafe.ReadUnaligned<TableOfContentsEntry>(ref ptr));
                buffer = buffer.Slice(Unsafe.SizeOf<TableOfContentsEntry>());
            }
        }

        private static void ReadBlockIndex(TableMetaData metaData, ReadOnlyMemory<byte> memory)
        {
            var data = memory.Span;
            var expectedLength = BinaryPrimitives.ReadInt32LittleEndian(data);
            data = data.Slice(sizeof(int));
            if (data.Length / sizeof(long) != expectedLength)
            {
                throw new IndexOutOfRangeException($"Block index was {data.Length} but expected {expectedLength * sizeof(long)}");
            }

            while (data.Length > 0)
            {
                var location = BinaryPrimitives.ReadInt64LittleEndian(data);
                data = data.Slice(sizeof(long));
                metaData._blockEntries.Add(location);
            }
        }

        private static void ReadFilter(TableMetaData metaData, ReadOnlyMemory<byte> memory) => metaData._filter.LoadFromBlock(memory);

        private static void ReadStatistics(TableMetaData metaData, ReadOnlyMemory<byte> memory)
        {
            var span = memory.Span;

            // Read first key
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
            var keyBuffer = new byte[keyLength];
            var tmpSpan = span.Slice(4, keyLength);
            tmpSpan.CopyTo(keyBuffer);
            metaData._firstKey = keyBuffer;
            span = span[(sizeof(int) + keyLength)..];

            keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
            keyBuffer = new byte[keyLength];
            span.Slice(4, keyLength).CopyTo(keyBuffer);
            metaData._lastKey = keyBuffer;
            span = span[(sizeof(int) + keyLength)..];
        }

        private static async Task<TOCMemory> GetTOCMemory(FileStream fs, TableMetaData metaData, TableOfContentsEntryType entryType)
        {
            var entry = metaData._tocEntries.Single(te => te.EntryType == entryType);
            var owner = MemoryPool<byte>.Shared.Rent(entry.Length);
            var mem = owner.Memory.Slice(0, entry.Length);
            await GetBlockFromFile(fs, entry.Offset, mem);
            return new TOCMemory() { Memory = mem, Owner = owner };
        }

        private static Task GetBlockFromFile(FileStream fs, long location, Memory<byte> memoryToFill)
        {
            fs.Seek(location, SeekOrigin.Begin);

            return GetBlockFromFile(fs, memoryToFill);
        }

        private static async Task GetBlockFromFile(FileStream fs, Memory<byte> memoryToFill)
        {
            var currentMemory = memoryToFill;

            while (currentMemory.Length > 0)
            {
                var result = await fs.ReadAsync(currentMemory);
                if (result == -1)
                {
                    throw new InvalidOperationException("Could not read any data from the file");
                }
                currentMemory = currentMemory.Slice(result);

            }
        }

        private struct TOCMemory : IDisposable
        {
            public Memory<byte> Memory;
            public IMemoryOwner<byte> Owner;

            public void Dispose() => Owner.Dispose();
        }
    }
}
