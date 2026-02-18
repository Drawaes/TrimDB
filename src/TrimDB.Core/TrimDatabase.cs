using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrimDB.Core.Hashing;
using TrimDB.Core.InMemory;
using TrimDB.Core.KVLog;
using TrimDB.Core.Storage;
using TrimDB.Core.Storage.Blocks;
using TrimDB.Core.Storage.Layers;

namespace TrimDB.Core
{
    public class TrimDatabase : IAsyncDisposable
    {
        private MemoryTable? _skipList;
        private MemoryTable? _nextMemoryTable;
        private List<MemoryTable> _oldInMemoryTables = new List<MemoryTable>();
        private readonly List<StorageLayer> _storageLayers = new List<StorageLayer>();
        private readonly IHashFunction _hasher = new MurmurHash3();
        private readonly SemaphoreSlim _skipListLock = new SemaphoreSlim(1);
        private readonly string _databaseFolder;
        private IOScheduler? _ioScheduler;
        private readonly BlockCache _blockCache;
        private TrimDatabaseOptions _options;
        private KVLogManager? _walManager;
        private ManifestManager? _manifest;
        private int _disposed;

        public TrimDatabase(TrimDatabaseOptions options)
        {
            _blockCache = options.BlockCache();
            _options = options;
            if (!System.IO.Directory.Exists(options.DatabaseFolder))
            {
                System.IO.Directory.CreateDirectory(options.DatabaseFolder);
            }

            _databaseFolder = options.DatabaseFolder;

            var unsorted = new UnsortedStorageLayer(1, _databaseFolder, _blockCache, _options.MaxL0Files);

            _storageLayers.Add(unsorted);

            for (var i = 2; i <= _options.Levels; i++)
            {
                var filesAtLevel = (int)Math.Ceiling(_options.FirstLevelMaxFileCount * Math.Pow(_options.LevelIncreaseFactor, i - 2));
                _storageLayers.Add(new SortedStorageLayer(i, _databaseFolder, _blockCache, _options.FileSize, filesAtLevel));
            }
            if (!_options.OpenReadOnly)
            {
                _skipList = _options.MemoryTable();
                _nextMemoryTable = _options.MemoryTable();

                if (!_options.DisableWAL)
                {
                    var walPath = System.IO.Path.Combine(_databaseFolder, "wal.wal");
                    _walManager = new FileBasedKVLogManager(walPath, waitForFlush: _options.WalWaitForFlush, channelCapacity: _options.WalChannelCapacity);
                    _walManager.OnBatchFlushed = async (batch) =>
                    {
                        foreach (var (op, offset) in batch)
                        {
                            if (op.IsCommitMarker) continue;
                            while (true)
                            {
                                var sl = Volatile.Read(ref _skipList);
                                if (sl == null) break; // disposed
                                bool ok = op.Deleted ? sl.Delete(op.Key.Span) : sl.Put(op.Key.Span, op.Value.Span);
                                if (ok) { sl.UpdateWalHighWatermark(offset); break; }
                                await SwitchInMemoryTable(sl);
                            }
                        }
                    };
                }
            }
        }

        internal TrimDatabaseOptions Options => _options;
        internal KVLogManager? WalManager => _walManager;

        public async Task LoadAsync()
        {
            // Initialize manifest if enabled
            ManifestData? manifestData = null;
            if (!_options.DisableManifest)
            {
                _manifest = new ManifestManager(_databaseFolder);
                _manifest.CleanupTempFile();
                if (_manifest.Exists)
                    manifestData = _manifest.TryRead();
            }

            // Clean up orphaned files BEFORE loading (orphans may be invalid SSTables)
            if (manifestData != null)
            {
                foreach (var sl in _storageLayers)
                {
                    var authoritative = new HashSet<int>(manifestData.GetFiles(sl.Level));
                    foreach (var tf in sl.GetTables())
                    {
                        if (!authoritative.Contains(tf.FileId.FileId))
                        {
                            sl.RemoveTable(tf);
                            System.IO.File.Delete(tf.FileName);
                        }
                    }
                }
            }

            foreach (var sl in _storageLayers)
            {
                await sl.LoadLayer().ConfigureAwait(false);
            }

            if (manifestData == null && _manifest != null)
            {
                // First run (no manifest yet): write initial manifest from current state
                await WriteManifestAsync();
            }

            if (!_options.OpenReadOnly)
            {
                if (_walManager != null)
                    await ReplayWalAsync();

                _ioScheduler = new IOScheduler(_options.MaxMemtableFlushBacklog, (UnsortedStorageLayer)_storageLayers[0], this);

                // Flush any memtables that filled during WAL replay
                if (_replayPendingFlush != null)
                {
                    foreach (var pendingSl in _replayPendingFlush)
                        await _ioScheduler.ScheduleSave(pendingSl);
                    _replayPendingFlush = null;
                }
            }
        }

        private List<MemoryTable>? _replayPendingFlush;

        private async Task ReplayWalAsync()
        {
            await foreach (var op in _walManager!.GetUncommittedOperations())
            {
                while (true)
                {
                    var sl = Volatile.Read(ref _skipList);
                    if (sl == null) break;
                    bool ok = op.Deleted ? sl.Delete(op.Key.Span) : sl.Put(op.Key.Span, op.Value.Span);
                    if (ok) break;
                    // Memtable full during replay — park it and install a fresh one
                    await SwitchInMemoryTableForReplay(sl);
                }
            }
        }

        private async Task SwitchInMemoryTableForReplay(MemoryTable sl)
        {
            await _skipListLock.WaitAsync();
            try
            {
                if (Volatile.Read(ref _skipList) == sl)
                {
                    var list = new List<MemoryTable>(_oldInMemoryTables) { sl };
                    Interlocked.Exchange(ref _oldInMemoryTables, list);

                    _replayPendingFlush ??= new List<MemoryTable>();
                    _replayPendingFlush.Add(sl);

                    var next = Interlocked.Exchange(ref _nextMemoryTable, null);
                    Interlocked.Exchange(ref _skipList, next ?? _options.MemoryTable());
                }
            }
            finally
            {
                _skipListLock.Release();
            }
        }

        internal List<StorageLayer> StorageLayers => _storageLayers;

        public BlockCache BlockCache => _blockCache;

        public ValueTask<ReadOnlyMemory<byte>> GetAsync(ReadOnlySpan<byte> key)
        {
            if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(TrimDatabase));

            TrimDbMetrics.Gets.Add(1);

            var sl = Volatile.Read(ref _skipList);
            if (sl != null)
            {
                var result = sl.TryGetMemory(key, out var value);

                if (result == SearchResult.Deleted)
                {
                    TrimDbMetrics.GetMemHits.Add(1);
                    return default;
                }
                else if (result == SearchResult.Found)
                {
                    TrimDbMetrics.GetMemHits.Add(1);
                    return new ValueTask<ReadOnlyMemory<byte>>(value);
                }
            }

            var oldLists = Volatile.Read(ref _oldInMemoryTables);
            foreach (var oldsl in oldLists)
            {
                var result = oldsl.TryGetMemory(key, out var value);
                if (result == SearchResult.Deleted)
                {
                    TrimDbMetrics.GetMemHits.Add(1);
                    return new ValueTask<ReadOnlyMemory<byte>>(new ReadOnlyMemory<byte>());
                }
                else if (result == SearchResult.Found)
                {
                    TrimDbMetrics.GetMemHits.Add(1);
                    return new ValueTask<ReadOnlyMemory<byte>>(value);
                }
            }

            var copiedMemory = MemoryPool<byte>.Shared.Rent(key.Length);
            key.CopyTo(copiedMemory.Memory.Span);
            if (TrimDbMetrics.GetDuration.Enabled)
                return TimedGetAsync(copiedMemory, key.Length, Stopwatch.GetTimestamp());
            return GetAsyncInternalWithRental(copiedMemory, key.Length);
        }

        internal void RemoveMemoryTable(MemoryTable sl)
        {
            while (true)
            {
                var memTable = _oldInMemoryTables;
                var list = new List<MemoryTable>(_oldInMemoryTables);
                list.Remove(sl);
                if (Interlocked.CompareExchange(ref _oldInMemoryTables, list, memTable) == memTable) return;
            }
        }

        internal async Task WriteManifestAsync()
        {
            if (_manifest == null) return;

            var data = new ManifestData();
            foreach (var sl in _storageLayers)
            {
                var indices = sl.GetFileIndices();
                foreach (var idx in indices)
                    data.AddFile(sl.Level, idx);
            }
            await _manifest.WriteAsync(data);
        }

        private async ValueTask<ReadOnlyMemory<byte>> GetAsyncInternalWithRental(IMemoryOwner<byte> rental, int keyLength)
        {
            try
            {
                return await GetAsyncInternal(rental.Memory.Slice(0, keyLength));
            }
            finally
            {
                rental.Dispose();
            }
        }

        private async ValueTask<ReadOnlyMemory<byte>> TimedGetAsync(IMemoryOwner<byte> rental, int keyLength, long startTimestamp)
        {
            try
            {
                return await GetAsyncInternalWithRental(rental, keyLength);
            }
            finally
            {
                TrimDbMetrics.GetDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
            }
        }

        public async ValueTask<ReadOnlyMemory<byte>> GetAsyncInternal(ReadOnlyMemory<byte> key)
        {
            var keyHash = _hasher.ComputeHash64(key.Span);

            foreach (var storage in _storageLayers)
            {
                var result = await storage.GetAsync(key, keyHash).ConfigureAwait(false);
                if (result.Result == SearchResult.Deleted || result.Result == SearchResult.Found)
                {
                    TrimDbMetrics.GetStorageHits.Add(1);
                    return result.Value;
                }
            }
            return default;
        }

        internal async ValueTask<SearchResult> DoesKeyExistBelowLevel(ReadOnlyMemory<byte> key, int levelId)
        {
            var keyHash = _hasher.ComputeHash64(key.Span);
            for (var i = levelId; i < _storageLayers.Count; i++)
            {
                var storage = _storageLayers[i];
                var result = await storage.GetAsync(key, keyHash);
                if (result.Result == SearchResult.Deleted || result.Result == SearchResult.Found)
                {
                    return result.Result;
                }
            }
            return SearchResult.NotFound;
        }

        public ValueTask<bool> DeleteAsync(ReadOnlySpan<byte> key)
        {
            if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(TrimDatabase));

            TrimDbMetrics.Deletes.Add(1);

            if (_walManager != null)
            {
                var keyCopy = key.ToArray(); // must copy — span can't cross await
                return new ValueTask<bool>(DeleteViaWalAsync(keyCopy));
            }

            // WAL disabled: existing direct path
            var sl = Volatile.Read(ref _skipList)!;
            if (sl.Delete(key))
            {
                return new ValueTask<bool>(true);
            }
            // Memtable full — copy key and go async to switch memtable
            var keyCopy2 = key.ToArray();
            return DeleteAsyncSlow(keyCopy2);
        }

        private async Task<bool> DeleteViaWalAsync(byte[] key)
        {
            await _walManager!.LogKV(key, Memory<byte>.Empty, isDeleted: true);
            return true; // memtable delete happened in OnBatchFlushed callback
        }

        private async ValueTask<bool> DeleteAsyncSlow(byte[] key)
        {
            while (true)
            {
                var sl = Volatile.Read(ref _skipList)!;
                if (sl.Delete(key))
                {
                    return true;
                }
                await SwitchInMemoryTable(sl);
            }
        }

        public async Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
        {
            if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(TrimDatabase));

            TrimDbMetrics.Puts.Add(1);
            long startTimestamp = 0;
            if (TrimDbMetrics.PutDuration.Enabled)
                startTimestamp = Stopwatch.GetTimestamp();

            try
            {
                if (_walManager != null)
                {
                    // Single-writer: WAL consumer handles memtable insert via callback
                    await _walManager.LogKV(MemoryMarshal.AsMemory(key), MemoryMarshal.AsMemory(value), false);
                    return;
                }

                // WAL disabled: direct memtable write (existing concurrent behavior)
                while (true)
                {
                    var sl = Volatile.Read(ref _skipList)!;

                    if (!sl.Put(key.Span, value.Span))
                    {
                        await SwitchInMemoryTable(sl);
                        continue;
                    }
                    return;
                }
            }
            finally
            {
                if (startTimestamp != 0)
                    TrimDbMetrics.PutDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
            }
        }

        public async IAsyncEnumerable<ScanEntry> ScanAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(TrimDatabase));

            TrimDbMetrics.Scans.Add(1);

            var sources = CollectScanSources(default, default, cancellationToken);
            if (sources.Count == 0) yield break;

            await using var merger = new TableFileMerger(sources.ToArray());
            while (await merger.MoveNextAsync().ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var item = merger.Current;
                if (item.IsDeleted) continue;
                yield return new ScanEntry(item.Key.ToArray(), item.Value.ToArray());
            }
        }

        public IAsyncEnumerable<ScanEntry> ScanAsync(
            ReadOnlyMemory<byte> startKey,
            CancellationToken cancellationToken = default)
        {
            return ScanAsync(startKey, default, cancellationToken);
        }

        public async IAsyncEnumerable<ScanEntry> ScanAsync(
            ReadOnlyMemory<byte> startKey,
            ReadOnlyMemory<byte> endKey,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(TrimDatabase));

            TrimDbMetrics.Scans.Add(1);

            var sources = CollectScanSources(startKey, endKey, cancellationToken);
            if (sources.Count == 0) yield break;

            bool hasStart = startKey.Length > 0;
            bool hasEnd = endKey.Length > 0;

            await using var merger = new TableFileMerger(sources.ToArray());
            while (await merger.MoveNextAsync().ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var item = merger.Current;
                if (item.IsDeleted) continue;

                if (hasStart && item.Key.SequenceCompareTo(startKey.Span) < 0) continue;
                if (hasEnd && item.Key.SequenceCompareTo(endKey.Span) > 0) yield break;

                yield return new ScanEntry(item.Key.ToArray(), item.Value.ToArray());
            }
        }

        private List<IAsyncEnumerator<IMemoryItem>> CollectScanSources(
            ReadOnlyMemory<byte> startKey,
            ReadOnlyMemory<byte> endKey,
            CancellationToken cancellationToken)
        {
            var sources = new List<IAsyncEnumerator<IMemoryItem>>();

            // Active memtable
            var sl = Volatile.Read(ref _skipList);
            if (sl != null)
                sources.Add(new SyncToAsyncEnumerator<IMemoryItem>(sl.GetEnumerator()));

            // Old memtables (newest first)
            var oldLists = Volatile.Read(ref _oldInMemoryTables);
            for (var i = oldLists.Count - 1; i >= 0; i--)
                sources.Add(new SyncToAsyncEnumerator<IMemoryItem>(oldLists[i].GetEnumerator()));

            // Storage layers (skip files entirely outside [startKey, endKey])
            bool hasStart = startKey.Length > 0;
            bool hasEnd = endKey.Length > 0;

            foreach (var layer in _storageLayers)
            {
                var tables = layer.GetTables();
                if (layer is UnsortedStorageLayer)
                {
                    for (var i = tables.Length - 1; i >= 0; i--)
                    {
                        if (hasStart && tables[i].LastKey.Span.SequenceCompareTo(startKey.Span) < 0) continue;
                        if (hasEnd && tables[i].FirstKey.Span.SequenceCompareTo(endKey.Span) > 0) continue;
                        sources.Add(tables[i].GetAsyncEnumerator(cancellationToken));
                    }
                }
                else
                {
                    foreach (var t in tables)
                    {
                        if (hasStart && t.LastKey.Span.SequenceCompareTo(startKey.Span) < 0) continue;
                        if (hasEnd && t.FirstKey.Span.SequenceCompareTo(endKey.Span) > 0) continue;
                        sources.Add(t.GetAsyncEnumerator(cancellationToken));
                    }
                }
            }

            return sources;
        }

        private async Task SwitchInMemoryTable(MemoryTable sl)
        {
            await _skipListLock.WaitAsync();
            try
            {
                if (Volatile.Read(ref _skipList) == sl)
                {
                    var list = new List<MemoryTable>(_oldInMemoryTables) { sl };
                    Interlocked.Exchange(ref _oldInMemoryTables, list);

                    TrimDbMetrics.Flushes.Add(1);

                    // Use the pre-allocated memtable for an instant swap
                    var next = Interlocked.Exchange(ref _nextMemoryTable, null);
                    Interlocked.Exchange(ref _skipList, next ?? _options.MemoryTable());

                    await _ioScheduler!.ScheduleSave(sl);

                    // Pre-allocate the next memtable in the background
                    _ = Task.Run(() => Volatile.Write(ref _nextMemoryTable, _options.MemoryTable()));
                }
            }
            finally
            {
                _skipListLock.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            if (_ioScheduler == null)
            {
                _blockCache.Dispose();
                return;
            }
            // Set the current in memory database to null
            await _skipListLock.WaitAsync();
            try
            {
                MemoryTable? old = null;
                old = Interlocked.Exchange(ref _skipList, old);
                await _ioScheduler.ScheduleSave(old!);
            }
            finally
            {
                _skipListLock.Release();
            }
            await _ioScheduler.DisposeAsync();
            if (_walManager != null) await _walManager.DisposeAsync();
            _blockCache.Dispose();
        }
    }
}
