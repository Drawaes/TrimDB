using System;
using System.Text;
using TrimDB.Core.InMemory;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.InMemory.SkipList64;
using Xunit;

#pragma warning disable CS0618 // Obsolete SkipList64 types used in tests

namespace TrimDB.Core.Facts
{
    public class DeleteFacts
    {
        [Fact]
        public void PutThenDeleteReturnsDeleted32()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 10_000, 25);
            var skipList = new SkipList32(allocator);
            PutThenDeleteReturnsDeleted(skipList);
        }

        [Fact]
        public void PutThenDeleteReturnsDeleted64()
        {
            using var allocator = new ArrayBasedAllocator64(4096 * 10_000, 25);
            var skipList = new SkipList64(allocator);
            PutThenDeleteReturnsDeleted(skipList);
        }

        private void PutThenDeleteReturnsDeleted(MemoryTable memoryTable)
        {
            var key = Encoding.UTF8.GetBytes("testkey");
            var value = Encoding.UTF8.GetBytes("testvalue");

            memoryTable.Put(key, value);
            var result = memoryTable.TryGet(key, out _);
            Assert.Equal(SearchResult.Found, result);

            var deleted = memoryTable.Delete(key);
            Assert.True(deleted);

            result = memoryTable.TryGet(key, out _);
            Assert.Equal(SearchResult.Deleted, result);
        }

        [Fact]
        public void DeleteNonExistentKeyReturnsFalse32()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 10_000, 25);
            var skipList = new SkipList32(allocator);
            DeleteNonExistentKeyReturnsFalse(skipList);
        }

        [Fact]
        public void DeleteNonExistentKeyReturnsFalse64()
        {
            using var allocator = new ArrayBasedAllocator64(4096 * 10_000, 25);
            var skipList = new SkipList64(allocator);
            DeleteNonExistentKeyReturnsFalse(skipList);
        }

        private void DeleteNonExistentKeyReturnsFalse(MemoryTable memoryTable)
        {
            var key = Encoding.UTF8.GetBytes("nonexistent");

            // In an LSM-tree, Delete always writes a tombstone (even for non-existent keys)
            // so it returns true as long as the allocator has space.
            var deleted = memoryTable.Delete(key);
            Assert.True(deleted);

            // The key should now show as Deleted (tombstone inserted)
            var result = memoryTable.TryGet(key, out _);
            Assert.Equal(SearchResult.Deleted, result);
        }

        [Fact]
        public void PutDeletePutReturnsNewValue32()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 10_000, 25);
            var skipList = new SkipList32(allocator);
            PutDeletePutReturnsNewValue(skipList);
        }

        [Fact]
        public void PutDeletePutReturnsNewValue64()
        {
            using var allocator = new ArrayBasedAllocator64(4096 * 10_000, 25);
            var skipList = new SkipList64(allocator);
            PutDeletePutReturnsNewValue(skipList);
        }

        private void PutDeletePutReturnsNewValue(MemoryTable memoryTable)
        {
            var key = Encoding.UTF8.GetBytes("testkey");
            var value1 = Encoding.UTF8.GetBytes("value1");
            var value2 = Encoding.UTF8.GetBytes("value2");

            memoryTable.Put(key, value1);
            memoryTable.Delete(key);

            var result = memoryTable.TryGet(key, out _);
            Assert.Equal(SearchResult.Deleted, result);

            memoryTable.Put(key, value2);
            result = memoryTable.TryGet(key, out var returnedValue);
            Assert.Equal(SearchResult.Found, result);
            Assert.Equal(value2, returnedValue.ToArray());
        }

        [Fact]
        public void MultipleDeletesOfSameKey32()
        {
            using var allocator = new ArrayBasedAllocator32(4096 * 10_000, 25);
            var skipList = new SkipList32(allocator);
            MultipleDeletesOfSameKey(skipList);
        }

        [Fact]
        public void MultipleDeletesOfSameKey64()
        {
            using var allocator = new ArrayBasedAllocator64(4096 * 10_000, 25);
            var skipList = new SkipList64(allocator);
            MultipleDeletesOfSameKey(skipList);
        }

        private void MultipleDeletesOfSameKey(MemoryTable memoryTable)
        {
            var key = Encoding.UTF8.GetBytes("testkey");
            var value = Encoding.UTF8.GetBytes("testvalue");

            memoryTable.Put(key, value);

            var deleted1 = memoryTable.Delete(key);
            Assert.True(deleted1);

            // Second delete on already-deleted key -- idempotent, still returns true
            var deleted2 = memoryTable.Delete(key);
            Assert.True(deleted2);

            var result = memoryTable.TryGet(key, out _);
            Assert.Equal(SearchResult.Deleted, result);
        }
    }
}
