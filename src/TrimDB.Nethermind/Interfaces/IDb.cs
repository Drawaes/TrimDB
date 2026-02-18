using System;
using System.Collections.Generic;

namespace TrimDB.Nethermind.Interfaces
{
    public interface IDb : IKeyValueStoreWithBatching, IDbMeta, IDisposable
    {
        string Name { get; }
        IEnumerable<KeyValuePair<byte[], byte[]?>> GetAll(bool ordered = false);
        IEnumerable<byte[]> GetAllKeys(bool ordered = false);
        IEnumerable<byte[]?> GetAllValues(bool ordered = false);
    }
}
