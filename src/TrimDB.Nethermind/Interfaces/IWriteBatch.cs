using System;

namespace TrimDB.Nethermind.Interfaces
{
    public interface IWriteBatch : IWriteOnlyKeyValueStore, IDisposable
    {
        void Clear();
    }
}
