using System;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.Hashing;
using TrimDB.Core.Storage.Filters;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class XorFilterFacts
    {
        private static readonly MurmurHash3 Hasher = new MurmurHash3();

        /// <summary>
        /// Test #91: Build filter from N keys. Every key used to build the filter
        /// passes MayContainKey. Zero false negatives. A single false negative is
        /// silent data loss.
        /// </summary>
        [Theory]
        [InlineData(100)]
        [InlineData(1000)]
        [InlineData(10000)]
        [Trait("Category", "Regression")]
        public void XorFilterZeroFalseNegatives(int count)
        {
            var filter = new XorFilter(count, useMurMur: true);
            var hashes = new long[count];

            for (int i = 0; i < count; i++)
            {
                var keyBytes = Encoding.UTF8.GetBytes($"key-{i}");
                filter.AddKey(keyBytes);
                hashes[i] = (long)Hasher.ComputeHash64(keyBytes);
            }

            // WriteToPipe triggers LoadFromKeys internally, but we need a PipeWriter.
            // Instead, call LoadFromKeys directly on the collected hashes via reflection
            // or just use the public API path: AddKey populates _keys, then we need to
            // trigger the build. The public path is WriteToPipe which needs a PipeWriter.
            //
            // Simpler: build directly from the hashes array.
            var directFilter = new XorFilter(count, useMurMur: true);
            directFilter.LoadFromKeys(hashes);

            for (int i = 0; i < count; i++)
            {
                Assert.True(directFilter.MayContainKey(hashes[i]),
                    $"False negative for key index {i} -- this is data loss");
            }
        }

        /// <summary>
        /// Test #92: Build filter from 10000 keys, query 10000 absent keys.
        /// False positive rate must be below 2% (theoretical for 8-bit fingerprint
        /// is ~0.39%).
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void XorFilterFalsePositiveRateBelow2Percent()
        {
            const int keyCount = 10_000;
            const int probeCount = 10_000;

            var hashes = new long[keyCount];
            for (int i = 0; i < keyCount; i++)
            {
                var keyBytes = Encoding.UTF8.GetBytes($"present-{i}");
                hashes[i] = (long)Hasher.ComputeHash64(keyBytes);
            }

            var filter = new XorFilter(keyCount, useMurMur: true);
            filter.LoadFromKeys(hashes);

            int falsePositives = 0;
            for (int i = 0; i < probeCount; i++)
            {
                // Keys that were NOT in the build set
                var absentKeyBytes = Encoding.UTF8.GetBytes($"absent-{i}");
                var absentHash = (long)Hasher.ComputeHash64(absentKeyBytes);

                if (filter.MayContainKey(absentHash))
                {
                    falsePositives++;
                }
            }

            double fpRate = (double)falsePositives / probeCount;
            Assert.True(fpRate < 0.02,
                $"False positive rate {fpRate:P2} exceeds 2% threshold ({falsePositives}/{probeCount})");
        }

        /// <summary>
        /// Test #93: Build filter from exactly 1 key. No division by zero,
        /// no out-of-bounds. The filter should work correctly.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void XorFilterWithSingleKey()
        {
            var keyBytes = Encoding.UTF8.GetBytes("only-key");
            var hash = (long)Hasher.ComputeHash64(keyBytes);
            var hashes = new long[] { hash };

            var filter = new XorFilter(1, useMurMur: true);
            filter.LoadFromKeys(hashes);

            Assert.True(filter.MayContainKey(hash), "Single key must be found in filter");

            // Also verify an absent key doesn't crash (it may or may not match, that's fine)
            var absentHash = (long)Hasher.ComputeHash64(Encoding.UTF8.GetBytes("not-in-filter"));
            _ = filter.MayContainKey(absentHash); // Must not throw
        }
    }
}
