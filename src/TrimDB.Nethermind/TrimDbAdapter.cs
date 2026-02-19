using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Nethermind.Core;
using Nethermind.Db;
using TrimDB.Core;

namespace TrimDB.Nethermind;

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

    public byte[]? Get(scoped ReadOnlySpan<byte> key, ReadFlags flags = ReadFlags.None)
    {
        ReadOnlyMemory<byte> result = _db.GetAsync(key).GetAwaiter().GetResult();
        return result.IsEmpty ? null : result.ToArray();
    }

    public bool KeyExists(ReadOnlySpan<byte> key)
    {
        ReadOnlyMemory<byte> result = _db.GetAsync(key).GetAwaiter().GetResult();
        return !result.IsEmpty;
    }

    public Span<byte> GetSpan(scoped ReadOnlySpan<byte> key, ReadFlags flags = ReadFlags.None)
    {
        // Dispose previous lease if any
        _currentLease.Dispose();

        ValueLease lease = _db.GetWithLeaseAsync(key).GetAwaiter().GetResult();
        if (!lease.IsFound || lease.IsDeleted)
        {
            lease.Dispose();
            _currentLease = default;
            return Span<byte>.Empty;
        }

        _currentLease = lease;
        // Safe: the lease keeps the underlying memory alive
        // Use MemoryMarshal to get a writable span from ReadOnlyMemory
        ref byte refByte = ref MemoryMarshal.GetReference(lease.Value.Span);
        return MemoryMarshal.CreateSpan(ref refByte, lease.Value.Length);
    }

    public void DangerousReleaseMemory(in ReadOnlySpan<byte> span)
    {
        _currentLease.Dispose();
        _currentLease = default;
    }

    public void Set(ReadOnlySpan<byte> key, byte[]? value, WriteFlags flags = WriteFlags.None)
    {
        if (value is null)
        {
            _db.DeleteAsync(key).GetAwaiter().GetResult();
        }
        else
        {
            _db.PutAsync(key.ToArray(), value).GetAwaiter().GetResult();
        }
    }

    public void Remove(ReadOnlySpan<byte> key)
    {
        _db.DeleteAsync(key).GetAwaiter().GetResult();
    }

    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, WriteFlags flags = WriteFlags.None)
    {
        // TrimDB has no native merge operator — fall back to last-write-wins Put
        _db.PutAsync(key.ToArray(), value.ToArray()).GetAwaiter().GetResult();
    }

    public IWriteBatch StartWriteBatch() => new TrimDbBatch(_db);

    public KeyValuePair<byte[], byte[]?>[] this[byte[][] keys]
    {
        get
        {
            KeyValuePair<byte[], byte[]?>[] result = new KeyValuePair<byte[], byte[]?>[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                result[i] = new KeyValuePair<byte[], byte[]?>(keys[i], Get(keys[i]));
            }
            return result;
        }
    }

    public IDbMeta.DbMetric GatherMetric()
    {
        TrimDbStats stats = _db.GetStats();
        return new IDbMeta.DbMetric { Size = stats.DiskBytes };
    }

    public void Flush(bool onlyWal = false) => _db.FlushAsync().GetAwaiter().GetResult();

    public void Compact() => _db.CompactAsync().GetAwaiter().GetResult();

    public void Clear()
    {
        // Not supported in TrimDB; no-op
    }

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

    public IEnumerable<byte[]> GetAllValues(bool ordered = false)
    {
        return _db.ScanAsync().ToBlockingEnumerable()
            .Select(e => e.Value.ToArray());
    }

    public void Tune(ITunableDb.TuneType type)
    {
        // No-op for TrimDB
    }

    public void Dispose()
    {
        _db.DisposeAsync().GetAwaiter().GetResult();
    }

    public ValueTask DisposeAsync() => _db.DisposeAsync();
}
