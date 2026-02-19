using System;
using System.Collections.Generic;
using Nethermind.Core;
using Nethermind.Db;

namespace TrimDB.Nethermind;

public sealed class TrimDbColumnsWriteBatch<TKey> : IColumnsWriteBatch<TKey> where TKey : struct, Enum
{
    private readonly TrimDbColumnsDb<TKey> _columnsDb;
    private readonly List<IWriteBatch> _batches = new();

    internal TrimDbColumnsWriteBatch(TrimDbColumnsDb<TKey> columnsDb)
    {
        _columnsDb = columnsDb;
    }

    public IWriteBatch GetColumnBatch(TKey key)
    {
        IWriteBatch batch = _columnsDb.GetColumnDb(key).StartWriteBatch();
        _batches.Add(batch);
        return batch;
    }

    public void Clear()
    {
        foreach (IWriteBatch batch in _batches)
            batch.Clear();
    }

    public void Dispose()
    {
        foreach (IWriteBatch batch in _batches)
            batch.Dispose();
        _batches.Clear();
    }
}
