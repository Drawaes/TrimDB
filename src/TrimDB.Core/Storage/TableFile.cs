using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class TableFile
    {
        private readonly string _fileName;
        private ReadOnlyMemory<byte> _toc;
        private ReadOnlyMemory<byte> _firstKey;
        private ReadOnlyMemory<byte> _lastKey;
        private TocEntry[] _tocEntries;
        private int _blockCount;
        private readonly int _index;
        private readonly int _level;

        public TableFile(string filename)
        {
            _fileName = filename;
            var numbers = Path.GetFileNameWithoutExtension(filename)["Level".Length..].Split('_');
            _level = int.Parse(numbers[0]);
            _index = int.Parse(numbers[1]);
        }

        public int Index => _index;
        public int Level => _level;

        internal ValueTask<(SearchResult result, Memory<byte> value)> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            var compare = key.Span.SequenceCompareTo(_firstKey.Span);
            if (compare < 0)
            {
                // Search is before the files range
                return ValueTasks.CreateResult(SearchResult.NotFound, default);
            }
            else if (compare == 0)
            {
                throw new NotImplementedException();
            }

            compare = key.Span.SequenceCompareTo(_lastKey.Span);
            if (compare > 0)
            {
                // Search is after the files range
                return ValueTasks.CreateResult(SearchResult.NotFound, default);
            }
            else if (compare == 0)
            {
                throw new NotImplementedException();
            }

            // Do a binary search

            throw new NotImplementedException();
        }

        public IEnumerator<TableItem> GetEnumerator() => new TableItemEnumerator(this);

        public async Task LoadAsync()
        {
            using var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
            var length = fs.Length;
            fs.Seek(-FileConsts.PageSize, SeekOrigin.End);

            var pageBuffer = await GetBlockFromFile(fs, FileConsts.PageSize);

            await ReadFooter(fs, pageBuffer);
            ReadToc();
            await LoadStatistics(fs);
            await LoadBlockIndex(fs);
        }

        private Task LoadBlockIndex(FileStream fs)
        {
            var blockIndex = _tocEntries.Single(te => te.EntryType == TocEntryType.BlockOffsets);
            _blockCount = blockIndex.Length / 12;
            return Task.CompletedTask;
        }

        private async Task LoadStatistics(FileStream fs)
        {
            var stats = _tocEntries.Single(te => te.EntryType == TocEntryType.Statistics);
            fs.Seek(stats.Offset, SeekOrigin.Begin);
            var memoryBlock = await GetBlockFromFile(fs, stats.Length);

            LoadStats(memoryBlock.Span);

            void LoadStats(ReadOnlySpan<byte> span)
            {
                // Read first key
                var keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
                // TODO sanity check on keylength?
                var keyBuffer = new byte[keyLength];
                var tmpSpan = span.Slice(4, keyLength);
                tmpSpan.CopyTo(keyBuffer);
                _firstKey = keyBuffer;
                span = span[(sizeof(int) + keyLength)..];

                keyLength = BinaryPrimitives.ReadInt32LittleEndian(span);
                keyBuffer = new byte[keyLength];
                span.Slice(4, keyLength).CopyTo(keyBuffer);
                _lastKey = keyBuffer;
                span = span[(sizeof(int) + keyLength)..];
            }
        }

        private void ReadToc()
        {
            var span = _toc.Span;

            span = span[..^FileConsts.TocEntryOffset];
            var numEntries = span.Length / Unsafe.SizeOf<TocEntry>();
            _tocEntries = new TocEntry[numEntries];

            ref var ptr = ref MemoryMarshal.GetReference(span);

            for (var i = 0; i < _tocEntries.Length; i++)
            {
                _tocEntries[i] = Unsafe.ReadUnaligned<TocEntry>(ref ptr);
                ptr = ref Unsafe.Add(ref ptr, Unsafe.SizeOf<TocEntry>());
            }
        }

        private async Task<ReadOnlyMemory<byte>> GetBlockFromFile(FileStream fs, int blockSize)
        {
            var pageBuffer = new byte[blockSize];

            var totalRead = 0;

            while (totalRead < blockSize)
            {
                var result = await fs.ReadAsync(pageBuffer, totalRead, pageBuffer.Length - totalRead);
                if (result == -1)
                {
                    throw new InvalidOperationException("Could not read any data from the file");
                }
                totalRead += result;
            }

            return pageBuffer;
        }

        private Task ReadFooter(FileStream fs, ReadOnlyMemory<byte> memory)
        {
            var buffer = memory.Span;
            var magicNumber = BinaryPrimitives.ReadUInt32LittleEndian(buffer[^4..]);
            if (magicNumber != FileConsts.MagicNumber)
            {
                throw new InvalidOperationException("The magic number for the file was incorrect");
            }

            var tocSize = BinaryPrimitives.ReadInt32LittleEndian(buffer[^8..]);
            var version = BinaryPrimitives.ReadInt32LittleEndian(buffer[^12..]);

            if (tocSize > FileConsts.PageSize)
            {
                return ReloadTOC(fs, tocSize);
            }
            else
            {
                _toc = memory[^tocSize..];
            }

            return Task.CompletedTask;

            async Task ReloadTOC(FileStream fs, int tocSize)
            {
                fs.Seek(-tocSize, SeekOrigin.End);
                _toc = await GetBlockFromFile(fs, tocSize);
            }
        }
    }
}
