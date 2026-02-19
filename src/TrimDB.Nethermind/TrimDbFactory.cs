using System;
using System.IO;
using Nethermind.Db;
using TrimDB.Core;

namespace TrimDB.Nethermind;

public class TrimDbFactory : IDbFactory
{
    private readonly string _baseDataDir;

    public TrimDbFactory(string baseDataDir)
    {
        _baseDataDir = baseDataDir;
    }

    public IDb CreateDb(DbSettings dbSettings)
    {
        string fullPath = GetFullDbPath(dbSettings);

        if (dbSettings.DeleteOnStart && Directory.Exists(fullPath))
            Directory.Delete(fullPath, recursive: true);

        Directory.CreateDirectory(fullPath);

        TrimDatabaseOptions options = new() { DatabaseFolder = fullPath };
        TrimDatabase db = new(options);
        db.LoadAsync().GetAwaiter().GetResult();
        return new TrimDbAdapter(db, dbSettings.DbName);
    }

    public IColumnsDb<T> CreateColumnsDb<T>(DbSettings dbSettings) where T : struct, Enum
    {
        string fullPath = GetFullDbPath(dbSettings);

        if (dbSettings.DeleteOnStart && Directory.Exists(fullPath))
            Directory.Delete(fullPath, recursive: true);

        Directory.CreateDirectory(fullPath);
        return new TrimDbColumnsDb<T>(fullPath);
    }

    public string GetFullDbPath(DbSettings dbSettings)
    {
        return Path.Combine(_baseDataDir, dbSettings.DbPath);
    }
}
