using System;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class AllocatorFacts
    {
        /// <summary>
        /// Test #84: Allocate a value, read it back via GetValue, bytes match.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void AllocateAndReadBackValue()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 100, 25);

            var original = Encoding.UTF8.GetBytes("hello world");
            var location = allocator.AllocateValue(original);

            Assert.NotEqual(0, location);

            var readBack = allocator.GetValue(location);
            Assert.Equal(original, readBack.ToArray());
        }

        /// <summary>
        /// Test #85: Allocate a node with a key, read the key back, matches.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void AllocateNodeAndReadBackKey()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 100, 25);

            var key = Encoding.UTF8.GetBytes("mykey");
            var node = allocator.AllocateNode(key);

            Assert.True(node.IsAllocated);
            Assert.Equal(key, node.Key.ToArray());
        }

        /// <summary>
        /// Test #86: HeadNode is valid immediately after allocator construction.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void HeadNodeExistsAtConstruction()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 100, 25);

            var head = allocator.HeadNode;
            Assert.NotEqual(0, head.Location);
            Assert.True(head.IsAllocated);
        }

        /// <summary>
        /// Test #87: Fill the allocator. Next allocation returns 0 (value) or
        /// unallocated node. No throw, no corruption of existing data.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public void AllocationFailsGracefullyWhenFull()
        {
            // Tiny allocator -- head node eats some space, then we fill the rest
            using var allocator = new ArrayBasedAllocator32(4096, 25);

            var key = Encoding.UTF8.GetBytes("k");
            var value = new byte[128];
            Array.Fill(value, (byte)0xAB);

            // Store a value we can verify later
            var firstLoc = allocator.AllocateValue(value);
            Assert.NotEqual(0, firstLoc);

            // Keep allocating until we fail
            int lastValueLoc;
            do
            {
                lastValueLoc = allocator.AllocateValue(value);
            }
            while (lastValueLoc != 0);

            // Value allocation returned 0 -- that's the graceful signal
            Assert.Equal(0, lastValueLoc);

            // Node allocation should also fail gracefully
            var failedNode = allocator.AllocateNode(Encoding.UTF8.GetBytes("this-should-fail"));
            Assert.False(failedNode.IsAllocated);

            // Original data is still intact
            var readBack = allocator.GetValue(firstLoc);
            Assert.Equal(value, readBack.ToArray());
        }
    }
}
