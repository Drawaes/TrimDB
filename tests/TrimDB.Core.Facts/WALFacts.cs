using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.KVLog;
using TrimDB.Core.Storage.Blocks.MemoryMappedCache;
using Xunit;

namespace TrimDB.Core.Facts
{
    public class WALFacts : IAsyncLifetime
    {
        private readonly string _logFile;

        public WALFacts()
        {
            _logFile = Path.GetTempFileName();
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public Task DisposeAsync()
        {
            try { if (File.Exists(_logFile)) File.Delete(_logFile); } catch { }
            return Task.CompletedTask;
        }

        // ====================================================================
        // WAL Unit Tests (should pass today)
        // ====================================================================

        /// <summary>
        /// Test #75: Log 100 KV pairs, read each back by offset. All correct.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public async Task LogMultipleKVPairs()
        {
            await using var kvLog = new FileBasedKVLogManager(_logFile);

            var offsets = new long[100];
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{i:D3}");
                var value = Encoding.UTF8.GetBytes($"value-{i:D3}");
                offsets[i] = await kvLog.LogKV(key, value, false);
            }

            // Read each value back by its entry-start offset
            for (int i = 0; i < 100; i++)
            {
                var expected = Encoding.UTF8.GetBytes($"value-{i:D3}");
                var actual = await kvLog.ReadValueAtLocation(offsets[i]);
                Assert.Equal(expected, actual.ToArray());
            }
        }

        /// <summary>
        /// Test #76: Log 5 entries. Commit first 2. GetUncommittedOperations returns
        /// entries 3-5 only.
        /// </summary>
        [Fact]
        [Trait("Category", "Bug")]
        public async Task GetUncommittedReturnsOnlyUncommitted()
        {
            await using var kvLog = new FileBasedKVLogManager(_logFile);

            var offsets = new long[5];
            for (int i = 0; i < 5; i++)
            {
                var key = Encoding.UTF8.GetBytes($"k{i}");
                var value = Encoding.UTF8.GetBytes($"v{i}");
                offsets[i] = await kvLog.LogKV(key, value, false);
            }

            // Commit through the second entry — offsets[2] is the start of entry 2,
            // which marks the boundary between committed and uncommitted entries
            await kvLog.RecordCommitted(offsets[2]);

            // Get uncommitted -- should be entries 2, 3, 4
            var uncommitted = new List<PutOperation>();
            await foreach (var op in kvLog.GetUncommittedOperations())
            {
                uncommitted.Add(op);
            }

            Assert.Equal(3, uncommitted.Count);
            Assert.Equal("k2", Encoding.UTF8.GetString(uncommitted[0].Key.ToArray()));
            Assert.Equal("k3", Encoding.UTF8.GetString(uncommitted[1].Key.ToArray()));
            Assert.Equal("k4", Encoding.UTF8.GetString(uncommitted[2].Key.ToArray()));
        }

        /// <summary>
        /// Test #77: Log a delete operation (isDeleted=true). The WAL writes tombstones
        /// with deleted=1 and valueLength=0. ReadValueAtLocation returns empty bytes.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task LogDeletedEntry()
        {
            await using var kvLog = new FileBasedKVLogManager(_logFile);

            var key = Encoding.UTF8.GetBytes("deleted-key");
            var value = Encoding.UTF8.GetBytes("deleted-value");

            // Should not throw when logging a delete
            var offset = await kvLog.LogKV(key, value, isDeleted: true);
            Assert.True(offset >= 0);

            // Tombstones are stored with valueLength=0, so ReadValueAtLocation returns empty
            var readBack = await kvLog.ReadValueAtLocation(offset);
            Assert.Empty(readBack.ToArray());
        }

        /// <summary>
        /// Test #78: Fresh WAL. IsAllCommitted returns true.
        /// A freshly opened WAL with the metadata file starting at offset 0 and an
        /// empty log file should report all committed.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task EmptyWALIsAllCommitted()
        {
            await using var kvLog = new FileBasedKVLogManager(_logFile);

            var result = await kvLog.IsAllCommitted();
            Assert.True(result, "A fresh WAL with no entries should report all committed");
        }

        /// <summary>
        /// Write entries, commit, dispose, reopen a new manager. Verify ReadOffset()
        /// returns the committed offset from the in-band marker (no metadata file).
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task CommitMarkerSurvivesReopen()
        {
            long committedOffset;

            // First session: write entries and commit
            await using (var kvLog = new FileBasedKVLogManager(_logFile))
            {
                for (int i = 0; i < 5; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"k{i}");
                    var value = Encoding.UTF8.GetBytes($"v{i}");
                    await kvLog.LogKV(key, value, false);
                }

                committedOffset = kvLog.CurrentPosition;
                await kvLog.RecordCommitted(committedOffset);
            }

            // Second session: reopen and verify
            await using var kvLog2 = new FileBasedKVLogManager(_logFile);
            var readBack = await kvLog2.ReadOffset();
            Assert.Equal(committedOffset, readBack);

            var allCommitted = await kvLog2.IsAllCommitted();
            Assert.True(allCommitted, "All entries were committed before reopen");
        }

        /// <summary>
        /// Write entries, commit at two different offsets. ReadOffset returns the last one.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task MultipleCommitMarkersLastWins()
        {
            await using var kvLog = new FileBasedKVLogManager(_logFile);

            // Write 3 entries
            for (int i = 0; i < 3; i++)
            {
                var key = Encoding.UTF8.GetBytes($"k{i}");
                var value = Encoding.UTF8.GetBytes($"v{i}");
                await kvLog.LogKV(key, value, false);
            }

            var firstCommit = kvLog.CurrentPosition;
            await kvLog.RecordCommitted(firstCommit);

            // Write 2 more entries
            for (int i = 3; i < 5; i++)
            {
                var key = Encoding.UTF8.GetBytes($"k{i}");
                var value = Encoding.UTF8.GetBytes($"v{i}");
                await kvLog.LogKV(key, value, false);
            }

            var secondCommit = kvLog.CurrentPosition;
            await kvLog.RecordCommitted(secondCommit);

            // ReadOffset should return the second (later) commit
            var readBack = await kvLog.ReadOffset();
            Assert.Equal(secondCommit, readBack);
            Assert.True(secondCommit > firstCommit);

            var allCommitted = await kvLog.IsAllCommitted();
            Assert.True(allCommitted);
        }

        /// <summary>
        /// Tombstones interleaved with commit markers: GetUncommittedOperations
        /// must yield tombstones correctly while skipping commit markers.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task TombstonesWithCommitMarkers()
        {
            await using var kvLog = new FileBasedKVLogManager(_logFile);

            // Write 2 normal puts + 1 delete (tombstone)
            await kvLog.LogKV(Encoding.UTF8.GetBytes("k0"), Encoding.UTF8.GetBytes("v0"), false);
            await kvLog.LogKV(Encoding.UTF8.GetBytes("k1"), Encoding.UTF8.GetBytes("v1"), false);
            var tombstoneOffset = await kvLog.LogKV(Encoding.UTF8.GetBytes("k2"), Encoding.UTF8.GetBytes("v2"), true);

            // Commit everything so far
            await kvLog.RecordCommitted(kvLog.CurrentPosition);

            // Write 1 more delete + 1 more put (uncommitted)
            await kvLog.LogKV(Encoding.UTF8.GetBytes("k3"), Encoding.UTF8.GetBytes("v3"), true);
            await kvLog.LogKV(Encoding.UTF8.GetBytes("k4"), Encoding.UTF8.GetBytes("v4"), false);

            // Get uncommitted — should be exactly 2 entries
            var uncommitted = new List<PutOperation>();
            await foreach (var op in kvLog.GetUncommittedOperations())
            {
                uncommitted.Add(op);
            }

            Assert.Equal(2, uncommitted.Count);
            Assert.Equal("k3", Encoding.UTF8.GetString(uncommitted[0].Key.ToArray()));
            Assert.True(uncommitted[0].Deleted, "k3 should be marked as deleted");
            Assert.Equal("k4", Encoding.UTF8.GetString(uncommitted[1].Key.ToArray()));
            Assert.False(uncommitted[1].Deleted, "k4 should not be deleted");

            // Verify the committed tombstone returns empty bytes
            var tombstoneValue = await kvLog.ReadValueAtLocation(tombstoneOffset);
            Assert.Empty(tombstoneValue.ToArray());
        }

        /// <summary>
        /// After committing all entries, GetUncommittedOperations yields zero items
        /// and IsAllCommitted returns true.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task GetUncommittedReturnsEmptyWhenAllCommitted()
        {
            await using var kvLog = new FileBasedKVLogManager(_logFile);

            // Write 3 entries
            for (int i = 0; i < 3; i++)
            {
                await kvLog.LogKV(
                    Encoding.UTF8.GetBytes($"k{i}"),
                    Encoding.UTF8.GetBytes($"v{i}"),
                    false);
            }

            // Commit at current position (all entries)
            await kvLog.RecordCommitted(kvLog.CurrentPosition);

            // Get uncommitted — should yield zero items
            var uncommitted = new List<PutOperation>();
            await foreach (var op in kvLog.GetUncommittedOperations())
            {
                uncommitted.Add(op);
            }

            Assert.Empty(uncommitted);
            Assert.True(await kvLog.IsAllCommitted());
        }

        /// <summary>
        /// Multiple reopen cycles with interleaved data and commit markers.
        /// Verifies ReadOffset, IsAllCommitted, and GetUncommittedOperations
        /// all work correctly across accumulated WAL contents.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task MultipleReopenCyclesMaintainState()
        {
            long session2CommitOffset;

            // Session 1: Write 3 entries, commit all, close
            await using (var kvLog = new FileBasedKVLogManager(_logFile))
            {
                for (int i = 0; i < 3; i++)
                {
                    await kvLog.LogKV(
                        Encoding.UTF8.GetBytes($"s1k{i}"),
                        Encoding.UTF8.GetBytes($"s1v{i}"),
                        false);
                }
                await kvLog.RecordCommitted(kvLog.CurrentPosition);
            }

            // Session 2: Reopen, write 2 more entries, commit all, close
            await using (var kvLog = new FileBasedKVLogManager(_logFile))
            {
                for (int i = 0; i < 2; i++)
                {
                    await kvLog.LogKV(
                        Encoding.UTF8.GetBytes($"s2k{i}"),
                        Encoding.UTF8.GetBytes($"s2v{i}"),
                        false);
                }
                session2CommitOffset = kvLog.CurrentPosition;
                await kvLog.RecordCommitted(session2CommitOffset);
            }

            // Session 3: Reopen, write 1 entry (NO commit), close
            await using (var kvLog = new FileBasedKVLogManager(_logFile))
            {
                await kvLog.LogKV(
                    Encoding.UTF8.GetBytes("s3k0"),
                    Encoding.UTF8.GetBytes("s3v0"),
                    false);
            }

            // Session 4: Reopen — verify state
            await using var kvLog4 = new FileBasedKVLogManager(_logFile);

            // ReadOffset should return session 2's commit position
            var readOffset = await kvLog4.ReadOffset();
            Assert.Equal(session2CommitOffset, readOffset);

            // IsAllCommitted should be false (1 uncommitted entry from session 3)
            Assert.False(await kvLog4.IsAllCommitted());

            // GetUncommittedOperations should yield exactly 1 entry (session 3's)
            var uncommitted = new List<PutOperation>();
            await foreach (var op in kvLog4.GetUncommittedOperations())
            {
                uncommitted.Add(op);
            }
            Assert.Single(uncommitted);
            Assert.Equal("s3k0", Encoding.UTF8.GetString(uncommitted[0].Key.ToArray()));
        }

        /// <summary>
        /// Tests the legacy metadata file fallback: (A) used when no in-band
        /// markers exist, and (B) in-band markers take precedence when both exist.
        /// </summary>
        [Fact]
        [Trait("Category", "Regression")]
        public async Task LegacyMetadataFallbackAndPrecedence()
        {
            var metaFile = _logFile + ".meta";
            try
            {
                var offsets = new long[3];

                // Write 3 entries (no commit markers)
                await using (var writer = new FileBasedKVLogManager(_logFile))
                {
                    for (int i = 0; i < 3; i++)
                    {
                        offsets[i] = await writer.LogKV(
                            Encoding.UTF8.GetBytes($"k{i}"),
                            Encoding.UTF8.GetBytes($"v{i}"),
                            false);
                    }
                }

                // Create legacy metadata file with committed offset = offsets[1]
                // (entry 0 is committed, entries 1 and 2 are uncommitted)
                var metaBytes = new byte[sizeof(long)];
                BinaryPrimitives.WriteInt64LittleEndian(metaBytes, offsets[1]);
                await File.WriteAllBytesAsync(metaFile, metaBytes);

                // Part A: fallback — no in-band markers, legacy metadata is used
                await using (var reader = new FileBasedKVLogManager(_logFile, metadataFileName: metaFile))
                {
                    var readOffset = await reader.ReadOffset();
                    Assert.Equal(offsets[1], readOffset);

                    var uncommitted = new List<PutOperation>();
                    await foreach (var op in reader.GetUncommittedOperations())
                    {
                        uncommitted.Add(op);
                    }
                    Assert.Equal(2, uncommitted.Count);
                    Assert.Equal("k1", Encoding.UTF8.GetString(uncommitted[0].Key.ToArray()));
                    Assert.Equal("k2", Encoding.UTF8.GetString(uncommitted[1].Key.ToArray()));
                }

                // Part B: precedence — add in-band marker, it should override legacy
                await using (var manager = new FileBasedKVLogManager(_logFile, metadataFileName: metaFile))
                {
                    await manager.RecordCommitted(manager.CurrentPosition);
                }

                await using (var verifier = new FileBasedKVLogManager(_logFile, metadataFileName: metaFile))
                {
                    var finalOffset = await verifier.ReadOffset();
                    Assert.True(finalOffset > offsets[1],
                        "In-band commit marker should take precedence over legacy metadata");
                    Assert.True(await verifier.IsAllCommitted());
                }
            }
            finally
            {
                try { if (File.Exists(metaFile)) File.Delete(metaFile); } catch { }
            }
        }

        // ====================================================================
        // WAL Integration Tests (will fail -- WAL not wired into TrimDatabase)
        // ====================================================================

        /// <summary>
        /// Test #79: PutAsync must write to WAL before returning. Verify WAL file
        /// contains the entry. Currently fails because WAL is not integrated.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task PutWritesToWALBeforeReturning()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_WAL79_" + Guid.NewGuid().ToString("N"));
            try
            {
                var options = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => new MMapBlockCache(),
                    DisableMerging = true,
                    DisableManifest = true,
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 1000, 25))
                };

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                var key = Encoding.UTF8.GetBytes("wal-test-key");
                var value = Encoding.UTF8.GetBytes("wal-test-value");
                await db.PutAsync(key, value);

                // If WAL is integrated, there should be a WAL file in the folder
                var walFiles = Directory.GetFiles(folder, "*.wal");
                Assert.True(walFiles.Length > 0, "No WAL file found after PutAsync -- WAL not integrated");

                await db.DisposeAsync();
            }
            finally
            {
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }

        /// <summary>
        /// Test #80: DeleteAsync must write tombstone to WAL.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task DeleteWritesToWALBeforeReturning()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_WAL80_" + Guid.NewGuid().ToString("N"));
            try
            {
                var options = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => new MMapBlockCache(),
                    DisableMerging = true,
                    DisableManifest = true,
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 1000, 25))
                };

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                var key = Encoding.UTF8.GetBytes("wal-del-key");
                var value = Encoding.UTF8.GetBytes("wal-del-value");
                await db.PutAsync(key, value);
                await db.DeleteAsync(key);

                var walFiles = Directory.GetFiles(folder, "*.wal");
                Assert.True(walFiles.Length > 0, "No WAL file found after DeleteAsync -- WAL not integrated");

                await db.DisposeAsync();
            }
            finally
            {
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }

        /// <summary>
        /// Test #81: Write entries to WAL, do NOT flush. "Crash" (dispose without flush).
        /// Reopen. Replay WAL into memtable. All entries readable.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task WALReplayReconstructsMemtable()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_WAL81_" + Guid.NewGuid().ToString("N"));
            try
            {
                // First session: write data
                var options = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => new MMapBlockCache(),
                    DisableMerging = true,
                    DisableManifest = true,
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 10_000, 25))
                };

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                for (int i = 0; i < 50; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"replay-key-{i:D3}");
                    var val = Encoding.UTF8.GetBytes($"replay-val-{i:D3}");
                    await db.PutAsync(key, val);
                }

                // Simulate crash: dispose without giving flush time to complete
                // In a real crash test this would be process kill
                await db.DisposeAsync();

                // Second session: reopen and verify WAL replay
                var db2 = new TrimDatabase(options);
                await db2.LoadAsync();

                for (int i = 0; i < 50; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"replay-key-{i:D3}");
                    var result = await db2.GetAsync(key);
                    Assert.True(result.Length > 0,
                        $"Key replay-key-{i:D3} not found after WAL replay");
                }

                await db2.DisposeAsync();
            }
            finally
            {
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }

        /// <summary>
        /// Test #82: Flush memtable to SSTable. WAL committed watermark advances.
        /// Old WAL entries eligible for cleanup.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task WALCheckpointAdvancesAfterFlush()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_WAL82_" + Guid.NewGuid().ToString("N"));
            try
            {
                var options = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => new MMapBlockCache(),
                    DisableMerging = true,
                    DisableManifest = true,
                    // Tiny allocator to trigger flush
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 50, 25))
                };

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                // Write enough to trigger a flush
                for (int i = 0; i < 200; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"ckpt-{i:D4}");
                    var val = Encoding.UTF8.GetBytes($"ckpt-v{i:D4}");
                    await db.PutAsync(key, val);
                }

                await Task.Delay(1000); // Let flush settle

                // After flush, WAL should have advanced its committed offset
                // This would be verifiable if WAL integration existed
                var walFiles = Directory.GetFiles(folder, "*.wal");
                Assert.True(walFiles.Length > 0,
                    "WAL checkpoint test requires WAL integration");

                await db.DisposeAsync();
            }
            finally
            {
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }

        /// <summary>
        /// Test #83: Two threads write interleaved keys. WAL sequence numbers must
        /// resolve last-writer-wins correctly on replay.
        /// </summary>
        [Fact]
        [Trait("Category", "Specification")]
        public async Task WALReplayRespectsConcurrentWriteOrdering()
        {
            var folder = Path.Combine(Path.GetTempPath(), "TrimDB_WAL83_" + Guid.NewGuid().ToString("N"));
            try
            {
                var options = new TrimDatabaseOptions
                {
                    DatabaseFolder = folder,
                    BlockCache = () => new MMapBlockCache(),
                    DisableMerging = true,
                    DisableManifest = true,
                    MemoryTable = () => new SkipList32(new ArrayBasedAllocator32(4096 * 10_000, 25))
                };

                var db = new TrimDatabase(options);
                await db.LoadAsync();

                // Two writers write the same key with different values
                var sharedKey = Encoding.UTF8.GetBytes("contested-key");
                var barrier = new System.Threading.CountdownEvent(2);

                var t1 = Task.Run(async () =>
                {
                    barrier.Signal();
                    barrier.Wait();
                    for (int i = 0; i < 100; i++)
                    {
                        await db.PutAsync(sharedKey, Encoding.UTF8.GetBytes($"t1-{i:D3}"));
                    }
                });

                var t2 = Task.Run(async () =>
                {
                    barrier.Signal();
                    barrier.Wait();
                    for (int i = 0; i < 100; i++)
                    {
                        await db.PutAsync(sharedKey, Encoding.UTF8.GetBytes($"t2-{i:D3}"));
                    }
                });

                await Task.WhenAll(t1, t2);

                // Read current value before "crash"
                var valueBeforeCrash = await db.GetAsync(sharedKey);
                var textBeforeCrash = Encoding.UTF8.GetString(valueBeforeCrash.ToArray());
                await db.DisposeAsync();

                // Reopen and verify WAL replay gives the same last-writer-wins result
                var db2 = new TrimDatabase(options);
                await db2.LoadAsync();

                var valueAfterReplay = await db2.GetAsync(sharedKey);
                Assert.True(valueAfterReplay.Length > 0,
                    "Key not found after WAL replay");

                // The value after replay should match what was there before the crash
                Assert.Equal(textBeforeCrash,
                    Encoding.UTF8.GetString(valueAfterReplay.ToArray()));

                await db2.DisposeAsync();
            }
            finally
            {
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }
    }
}
