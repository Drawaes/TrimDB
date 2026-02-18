namespace TrimDB.Core
{
    public readonly struct TrimDbStats
    {
        public long DiskBytes { get; init; }
        public int SstableCount { get; init; }
        public int LevelCount { get; init; }
    }
}
