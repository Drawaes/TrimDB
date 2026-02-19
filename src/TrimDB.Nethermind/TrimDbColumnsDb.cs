using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Nethermind.Core;
using Nethermind.Db;
using TrimDB.Core;

namespace TrimDB.Nethermind;

public class TrimDbColumnsDb<TKey> : IColumnsDb<TKey>, ITunableDb where TKey : struct, Enum
{
    private readonly ConcurrentDictionary<TKey, TrimDbAdapter> _columns = new();
    private readonly string _basePath;

    public TrimDbColumnsDb(string basePath)
    {
        _basePath = basePath;
    }

    public IDb GetColumnDb(TKey key)
    {
        return _columns.GetOrAdd(key, static (k, basePath) =>
        {
            string columnPath = Path.Combine(basePath, k.ToString()!);
            Directory.CreateDirectory(columnPath);
            TrimDatabaseOptions options = new() { DatabaseFolder = columnPath };
            TrimDatabase db = new(options);
            db.LoadAsync().GetAwaiter().GetResult();
            return new TrimDbAdapter(db, k.ToString()!);
        }, _basePath);
    }

    public IEnumerable<TKey> ColumnKeys => Enum.GetValues<TKey>();

    public IColumnsWriteBatch<TKey> StartWriteBatch() => new TrimDbColumnsWriteBatch<TKey>(this);

    public IColumnDbSnapshot<TKey> CreateSnapshot() => throw new NotSupportedException("Snapshots are not supported by TrimDB");

    public IDbMeta.DbMetric GatherMetric()
    {
        long totalSize = 0;
        foreach (TrimDbAdapter adapter in _columns.Values)
        {
            IDbMeta.DbMetric metric = adapter.GatherMetric();
            totalSize += metric.Size;
        }
        return new IDbMeta.DbMetric { Size = totalSize };
    }

    public void Flush(bool onlyWal = false)
    {
        foreach (TrimDbAdapter adapter in _columns.Values)
            adapter.Flush(onlyWal);
    }

    public void Clear()
    {
        foreach (TrimDbAdapter adapter in _columns.Values)
            adapter.Clear();
    }

    public void Compact()
    {
        foreach (TrimDbAdapter adapter in _columns.Values)
            adapter.Compact();
    }

    public void Tune(ITunableDb.TuneType type)
    {
        // No-op for TrimDB
    }

    public void Dispose()
    {
        foreach (TrimDbAdapter adapter in _columns.Values)
            adapter.Dispose();
        _columns.Clear();
    }
}
