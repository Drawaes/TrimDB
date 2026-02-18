namespace TrimDB.Nethermind.Interfaces
{
    public readonly struct DbMetric
    {
        public string Name { get; init; }
        public long Value { get; init; }
    }
}
