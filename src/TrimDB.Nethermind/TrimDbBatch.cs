using System;
using Nethermind.Core;
using TrimDB.Core;

namespace TrimDB.Nethermind;

public sealed class TrimDbBatch : IWriteBatch
{
    private readonly TrimDatabase _db;
    private WriteBatch? _batch = new();

    internal TrimDbBatch(TrimDatabase db)
    {
        _db = db;
    }

    public void Set(ReadOnlySpan<byte> key, byte[]? value, WriteFlags flags = WriteFlags.None)
    {
        if (_batch is null) throw new ObjectDisposedException(nameof(TrimDbBatch));

        if (value is null)
            _batch.Delete(key);
        else
            _batch.Put(key, value);
    }

    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, WriteFlags flags = WriteFlags.None)
    {
        if (_batch is null) throw new ObjectDisposedException(nameof(TrimDbBatch));

        // TrimDB has no native merge — fall back to last-write-wins Put
        _batch.Put(key, value.ToArray());
    }

    public void Remove(ReadOnlySpan<byte> key)
    {
        if (_batch is null) throw new ObjectDisposedException(nameof(TrimDbBatch));
        _batch.Delete(key);
    }

    public void Clear()
    {
        _batch = new WriteBatch();
    }

    public void Dispose()
    {
        if (_batch is not null)
        {
            _db.ApplyBatchAsync(_batch).GetAwaiter().GetResult();
            _batch = null;
        }
    }
}
