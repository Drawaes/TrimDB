using System;
using System.Collections.Generic;
using System.Text;
using TrimDB.Core.InMemory.SkipList64;
using Xunit;

#pragma warning disable CS0618 // Obsolete SkipList64 types used in tests

namespace TrimDB.Core.Facts
{
    public class SkipListNodeFacts
    {
        [Theory]
        [InlineData(3, 10, 50)]
        public void SizeCalculatedCorrectly(byte height, int keyLength, int result)
        {
            Assert.Equal(result, SkipListNode64.CalculateSizeNeeded(height, keyLength));
        }

        [Fact]
        public void TestValueLocation()
        {
            var height = (byte)2;
            var key = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var memory = new byte[SkipListNode64.CalculateSizeNeeded(height, key.Length)];

            var node = new SkipListNode64(memory, 0);

            var valueLocation = 1000;

            node.SetValueLocation(valueLocation);

            Assert.Equal(valueLocation, node.ValueLocation);

            Assert.True(node.SetValueLocation(valueLocation, 500));

            Assert.False(node.SetValueLocation(valueLocation, 2000));
        }

        [Fact]
        public void SetupNewNode()
        {
            var height = (byte)2;
            var key = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var memory = new byte[SkipListNode64.CalculateSizeNeeded(height, key.Length)];

            var node = new SkipListNode64(memory, 77, height, key);

            Assert.Equal(height, node.TableHeight);
            Assert.True(node.Key.SequenceCompareTo(key) == 0);
            Assert.Equal(77, node.Location);
            Assert.True(node.IsAllocated);
        }

        [Fact]
        public void SetTableLocations()
        {
            var height = (byte)2;
            var key = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var memory = new byte[SkipListNode64.CalculateSizeNeeded(height, key.Length)];

            var node = new SkipListNode64(memory, 77, height, key);

            var tableLocation = 123456;
            var nextLocation = 234567;
            node.SetTablePointer(0, tableLocation);
            Assert.Equal(tableLocation, node.GetTableLocation(0));
            Assert.True(node.SetTablePointer(0, tableLocation, nextLocation));
            Assert.False(node.SetTablePointer(0, tableLocation, nextLocation));

            node.SetTablePointer(1, tableLocation);

            Assert.Equal(tableLocation, node.GetTableLocation(1));
            Assert.Equal(nextLocation, node.GetTableLocation(0));
        }
    }
}
