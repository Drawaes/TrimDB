using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class MergeIteratorFacts
    {
        [Fact]
        public void CheckMergeWorks()
        {
            var firstList = new List<IMemoryItem>() { new TestMemoryItem(1), new TestMemoryItem(5), new TestMemoryItem(7), new TestMemoryItem(10) };
            var secondList = new List<IMemoryItem>() { new TestMemoryItem(1), new TestMemoryItem(6), new TestMemoryItem(7), new TestMemoryItem(10), new TestMemoryItem(12) };

            var merger = new TableFileMerger(new IEnumerator<IMemoryItem>[] { firstList.GetEnumerator(), secondList.GetEnumerator() });
            var result = new List<int>();

            while (merger.MoveNext())
            {
                result.Add(merger.Current.Key[0]);
            }

            var expected = new List<int>() { 1, 5, 6, 7, 10, 12 };

            Assert.Equal(expected, result);
        }

        internal class TestMemoryItem : IMemoryItem
        {
            public TestMemoryItem(int keyNumber) => KeyNumber = keyNumber;

            public int KeyNumber { get; set; }

            public ReadOnlySpan<byte> Key => new byte[] { (byte)KeyNumber };

            public ReadOnlySpan<byte> Value => default;

            public bool IsDeleted => false;
        }
    }
}
