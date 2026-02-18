using System.Diagnostics.Metrics;

namespace TrimDB.Core
{
    internal static class TrimDbMetrics
    {
        private static readonly Meter s_meter = new("TrimDB.Core", "1.0.0");

        internal static readonly Counter<long> Puts = s_meter.CreateCounter<long>("db.puts");
        internal static readonly Counter<long> Gets = s_meter.CreateCounter<long>("db.gets");
        internal static readonly Counter<long> GetMemHits = s_meter.CreateCounter<long>("db.gets.memtable_hits");
        internal static readonly Counter<long> GetStorageHits = s_meter.CreateCounter<long>("db.gets.storage_hits");
        internal static readonly Counter<long> Deletes = s_meter.CreateCounter<long>("db.deletes");
        internal static readonly Counter<long> Scans = s_meter.CreateCounter<long>("db.scans");
        internal static readonly Counter<long> Flushes = s_meter.CreateCounter<long>("db.flushes");
        internal static readonly Counter<long> Compactions = s_meter.CreateCounter<long>("db.compactions");
        internal static readonly Histogram<double> PutDuration = s_meter.CreateHistogram<double>("db.put.duration", "ms");
        internal static readonly Histogram<double> GetDuration = s_meter.CreateHistogram<double>("db.get.duration", "ms");
    }
}
