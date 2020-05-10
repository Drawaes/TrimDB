using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class MergeIteratorFacts
    {
        [Fact]
        public async Task CheckMergeWorks()
        {
            var firstList = GetAsync(new byte[] { 1, 5, 7, 10 });
            var secondList = GetAsync(new byte[] { 1, 6, 7, 10, 12 });

            var merger = new TableFileMerger(new[] { firstList.GetAsyncEnumerator(), secondList.GetAsyncEnumerator() });
            var result = new List<int>();

            while (await merger.MoveNextAsync())
            {
                result.Add(merger.Current.Key[0]);
            }

            var expected = new List<int>() { 1, 5, 6, 7, 10, 12 };

            Assert.Equal(expected, result);
        }

        [Fact]
        public async Task OverlappedThreeWay()
        {
            var firstList = GetAsync(new byte[] { 10, 20, 30, 40, 50, 60 });
            var secondList = GetAsync(new byte[] { 5, 10, 20, 30 });
            var thirdList = GetAsync(new byte[] { 50, 60, 70, 80, 90, 100, 110 });

            var merger = new TableFileMerger(new[] { firstList.GetAsyncEnumerator(), secondList.GetAsyncEnumerator(), thirdList.GetAsyncEnumerator() });
            var result = new List<int>();

            while (await merger.MoveNextAsync())
            {
                result.Add(merger.Current.Key[0]);
            }

            var expected = new List<int>() { 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110 };

            Assert.Equal(expected, result);
        }

        internal class TestMemoryItem : IMemoryItem
        {
            public TestMemoryItem(int keyNumber) => KeyNumber = keyNumber;

            public int KeyNumber { get; set; }
            public byte ValueNumber { get; set; }

            public ReadOnlySpan<byte> Key => new byte[] { (byte)KeyNumber };

            public ReadOnlySpan<byte> Value => new byte[] { ValueNumber };

            public bool IsDeleted => false;
        }

        private static IAsyncEnumerable<IMemoryItem> GetAsync(byte[] items)
        {
            var firstList = System.Threading.Channels.Channel.CreateUnbounded<IMemoryItem>();
            foreach (var i in items)
            {
                firstList.Writer.TryWrite(new TestMemoryItem(i));
            }
            firstList.Writer.Complete();
            return firstList.Reader.ReadAllAsync();
        }
    }
}
