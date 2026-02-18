namespace TrimDB.Nethermind.Interfaces
{
    public interface IKeyValueStoreWithBatching : IKeyValueStore
    {
        IWriteBatch StartWriteBatch();
    }
}
