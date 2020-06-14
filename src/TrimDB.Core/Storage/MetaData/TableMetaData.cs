using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Pipelines;
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
        private List<(long offset, ReadOnlyMemory<byte> firstKey)> _blockEntries = new List<(long, ReadOnlyMemory<byte>)>();
        private Filter _filter;

        public int BlockCount => _blockEntries.Count;
        public ReadOnlyMemory<byte> FirstKey { get; set; }
        public ReadOnlyMemory<byte> LastKey { get; set; }
        public Filter Filter => _filter;
        public int Count { get; set; }

        public TableMetaData(int approxSize, bool useMurMur)
        {
            _filter = new XorFilter(approxSize, useMurMur);
        }

        public void AddTableEntry(long offset, int length, TableOfContentsEntryType entryType) => _tocEntries.Add(new TableOfContentsEntry() { EntryType = entryType, Length = length, Offset = offset });

        public int FindContainingBlock(ReadOnlyMemory<byte> key)
        {
            var result = _blockEntries.BinarySearch((0, key), CompareIndex.Instance);
            if (result < 0)
            {
                result = ~result;
                return result - 1;
            }
            return result;
        }

        private class CompareIndex : IComparer<(long offset, ReadOnlyMemory<byte> key)>
        {
            public static readonly CompareIndex Instance = new CompareIndex();

            private CompareIndex() { }

            public int Compare((long offset, ReadOnlyMemory<byte> key) x, (long offset, ReadOnlyMemory<byte> key) y) => x.key.Span.SequenceCompareTo(y.key.Span);
        }

        public static async Task<TableMetaData> LoadFromFileAsync(string fileName)
        {
            await using var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
            var metaData = new TableMetaData(0, true);

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

            while (data.Length > 0)
            {
                var location = BinaryPrimitives.ReadInt64LittleEndian(data);
                data = data.Slice(sizeof(long));
                var key = new byte[BinaryPrimitives.ReadInt32LittleEndian(data)];
                data = data.Slice(sizeof(int));
                data.Slice(0, key.Length).CopyTo(key);
                data = data.Slice(key.Length);
                metaData._blockEntries.Add((location, key));
            }
        }

        public void AddBlockOffset(long blockOffset, ReadOnlyMemory<byte> firstKey)
        {
            _blockEntries.Add((blockOffset, firstKey));
        }

        private static void ReadFilter(TableMetaData metaData, ReadOnlyMemory<byte> memory) => metaData._filter.LoadFromBlock(memory);

        private static void ReadStatistics(TableMetaData metaData, ReadOnlyMemory<byte> memory)
        {
            var span = memory.Span;

            // Read first key
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
            var keyBuffer = new byte[keyLength];
            var tmpSpan = span.Slice(sizeof(int), keyLength);
            tmpSpan.CopyTo(keyBuffer);
            metaData.FirstKey = keyBuffer;
            span = span[(sizeof(int) + keyLength)..];

            keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
            keyBuffer = new byte[keyLength];
            span.Slice(4, keyLength).CopyTo(keyBuffer);
            metaData.LastKey = keyBuffer;
            span = span[(sizeof(int) + keyLength)..];
        }

        public int WriteBlockOffsets(PipeWriter pipeWriter)
        {
            var sizeToWrite = _blockEntries.Sum(be => be.firstKey.Length + sizeof(long) + sizeof(int)) + sizeof(int);

            var span = pipeWriter.GetSpan(sizeToWrite);
            var totalSpan = span[..sizeToWrite];

            BinaryPrimitives.WriteInt32LittleEndian(span, _blockEntries.Count);
            span = span[sizeof(int)..];

            foreach (var be in _blockEntries)
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, be.offset);
                span = span[sizeof(long)..];
                BinaryPrimitives.WriteInt32LittleEndian(span, be.firstKey.Length);
                span = span[sizeof(int)..];
                be.firstKey.Span.CopyTo(span);
                span = span[be.firstKey.Length..];
            }

            pipeWriter.Advance(sizeToWrite);
            return sizeToWrite;
        }

        public void WriteTOC(PipeWriter filePipe)
        {
            var tocSize = _tocEntries.Count * Unsafe.SizeOf<TableOfContentsEntry>();
            tocSize += sizeof(uint) + sizeof(int) + sizeof(int);

            var span = filePipe.GetSpan(tocSize);
            span = span[..tocSize];
            var totalSpan = span;

            foreach (var te in _tocEntries)
            {
                span = WriteTOCEntry(span, te.EntryType, te.Offset, te.Length);
            }

            BinaryPrimitives.WriteInt32LittleEndian(span, FileConsts.Version);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteInt32LittleEndian(span, tocSize);
            span = span[sizeof(int)..];

            BinaryPrimitives.WriteUInt32LittleEndian(span, FileConsts.MagicNumber);
            span = span[sizeof(uint)..];

            filePipe.Advance(tocSize);
        }

        public static Span<byte> WriteTOCEntry(Span<byte> span, TableOfContentsEntryType tocType, long offset, int length)
        {
            var tocEntry = new TableOfContentsEntry() { EntryType = tocType, Offset = offset, Length = length };
            Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(span), tocEntry);
            return span[Unsafe.SizeOf<TableOfContentsEntry>()..];
        }

        public int WriteStats(PipeWriter pipeWriter)
        {
            var totalWritten = FirstKey.Length + (sizeof(int) * 3) + LastKey.Length;

            var memory = pipeWriter.GetSpan(totalWritten);
            var totalSpan = memory[..totalWritten];

            BinaryPrimitives.WriteInt32LittleEndian(memory, FirstKey.Length);
            memory = memory[sizeof(int)..];
            FirstKey.Span.CopyTo(memory);
            memory = memory[FirstKey.Length..];

            BinaryPrimitives.WriteInt32LittleEndian(memory, LastKey.Length);
            memory = memory[sizeof(int)..];
            LastKey.Span.CopyTo(memory);
            memory = memory[LastKey.Length..];

            BinaryPrimitives.WriteInt32LittleEndian(memory, Count);
            memory = memory[sizeof(int)..];

            pipeWriter.Advance(totalWritten);

            return totalWritten;
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
