using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.Storage.Blocks;

namespace TrimDB.Core.Storage.Layers
{
    public class SortedStorageLayer : StorageLayer
    {
        public SortedStorageLayer(int level, string databaseFolder, BlockCache blockCache, int targetFileSize, int maxFiles)
            : base(databaseFolder, level, blockCache, targetFileSize)
        {
            MaxFilesAtLayer = maxFiles;
        }

        public override int MaxFilesAtLayer { get; }

        public override int NumberOfTables => _tableFiles.Length;

        public override async ValueTask<SearchResultValue> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            var tfs = _tableFiles;
            var index = FindCandidateFile(tfs, key.Span);
            if (index < 0)
                return new SearchResultValue() { Result = SearchResult.NotFound };

            return await tfs[index].GetAsync(key, hash);
        }

        public override async ValueTask<ValueLease> GetWithLeaseAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            var tfs = _tableFiles;
            var index = FindCandidateFile(tfs, key.Span);
            if (index < 0)
                return ValueLease.Empty;

            return await tfs[index].GetWithLeaseAsync(key, hash);
        }

        private static int FindCandidateFile(TableFile[] files, ReadOnlySpan<byte> key)
        {
            int lo = 0, hi = files.Length - 1;
            while (lo <= hi)
            {
                var mid = lo + ((hi - lo) >> 1);
                if (key.SequenceCompareTo(files[mid].FirstKey.Span) < 0)
                    { hi = mid - 1; continue; }
                if (key.SequenceCompareTo(files[mid].LastKey.Span) > 0)
                    { lo = mid + 1; continue; }
                return mid;
            }
            return -1;
        }

        public IEnumerable<(ReadOnlyMemory<byte> firstKey, ReadOnlyMemory<byte> lastKey)> GetFirstAndLastKeys()
        {
            var tfs = _tableFiles;
            foreach (var tf in tfs)
            {
                yield return (tf.FirstKey, tf.LastKey);
            }
        }
    }
}
