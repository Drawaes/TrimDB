using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TrimDB.Core;
using TrimDB.Nethermind.Interfaces;

namespace TrimDB.Nethermind
{
    public class TrimDbAdapter : IDb, ITunableDb, IAsyncDisposable
    {
        private readonly TrimDatabase _db;

        [ThreadStatic]
        private static ValueLease _currentLease;

        public TrimDbAdapter(TrimDatabase db, string name)
        {
            _db = db;
            Name = name;
        }

        public string Name { get; }

        public byte[]? Get(ReadOnlySpan<byte> key, ReadFlags flags = ReadFlags.None)
        {
            var result = _db.GetAsync(key).GetAwaiter().GetResult();
            return result.IsEmpty ? null : result.ToArray();
        }

        public bool KeyExists(ReadOnlySpan<byte> key)
        {
            var result = _db.GetAsync(key).GetAwaiter().GetResult();
            return !result.IsEmpty;
        }

        public Span<byte> GetSpan(ReadOnlySpan<byte> key, ReadFlags flags = ReadFlags.None)
        {
            // Dispose previous lease if any
            _currentLease.Dispose();

            var lease = _db.GetWithLeaseAsync(key).GetAwaiter().GetResult();
            if (!lease.IsFound || lease.IsDeleted)
            {
                lease.Dispose();
                _currentLease = default;
                return Span<byte>.Empty;
            }

            _currentLease = lease;
            // Safe: the lease keeps the underlying memory alive
            return System.Runtime.InteropServices.MemoryMarshal.AsMemory(lease.Value).Span;
        }

        public void DangerousReleaseMemory(in ReadOnlySpan<byte> span)
        {
            _currentLease.Dispose();
            _currentLease = default;
        }

        public void Set(ReadOnlySpan<byte> key, byte[]? value, WriteFlags flags = WriteFlags.None)
        {
            if (value == null)
            {
                _db.DeleteAsync(key, flags).GetAwaiter().GetResult();
            }
            else
            {
                _db.PutAsync(key.ToArray(), value, flags).GetAwaiter().GetResult();
            }
        }

        public void Remove(ReadOnlySpan<byte> key)
        {
            _db.DeleteAsync(key).GetAwaiter().GetResult();
        }

        public IWriteBatch StartWriteBatch() => new TrimDbBatch(_db);

        public void Flush() => _db.FlushAsync().GetAwaiter().GetResult();

        public void Compact() => _db.CompactAsync().GetAwaiter().GetResult();

        public void Clear()
        {
            // Not supported in TrimDB; no-op
        }

        public IEnumerable<DbMetric> GetMetrics()
        {
            var stats = _db.GetStats();
            yield return new DbMetric { Name = "DiskBytes", Value = stats.DiskBytes };
            yield return new DbMetric { Name = "SstableCount", Value = stats.SstableCount };
            yield return new DbMetric { Name = "LevelCount", Value = stats.LevelCount };
        }

        public long GetSize() => _db.GetStats().DiskBytes;

        public IEnumerable<KeyValuePair<byte[], byte[]?>> GetAll(bool ordered = false)
        {
            return _db.ScanAsync().ToBlockingEnumerable()
                .Select(e => new KeyValuePair<byte[], byte[]?>(e.Key.ToArray(), e.Value.ToArray()));
        }

        public IEnumerable<byte[]> GetAllKeys(bool ordered = false)
        {
            return _db.ScanAsync().ToBlockingEnumerable()
                .Select(e => e.Key.ToArray());
        }

        public IEnumerable<byte[]?> GetAllValues(bool ordered = false)
        {
            return _db.ScanAsync().ToBlockingEnumerable()
                .Select(e => (byte[]?)e.Value.ToArray());
        }

        public void Tune(TuneType type)
        {
            // No-op for TrimDB
        }

        public void Dispose()
        {
            _db.DisposeAsync().GetAwaiter().GetResult();
        }

        public System.Threading.Tasks.ValueTask DisposeAsync() => _db.DisposeAsync();
    }
}
