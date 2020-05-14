using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Tracing;
using System.Text;
using System.Threading;

namespace TrimDB.Core.Storage.Blocks.CachePrototype
{
    [EventSource(Name = "ProtoBlockCache")]
    public class ProtoEventSource : EventSource
    {
        private EventCounter _cacheHits;
        private EventCounter _cacheMisses;
        private int _cacheHitsCount;
        private int _cacheMissCount;
        public static readonly ProtoEventSource Log = new ProtoEventSource();

        public ProtoEventSource()
        {
            _cacheHits = new EventCounter("CacheHits", this);
            _cacheMisses = new EventCounter("CacheMisses", this);
        }

        public void ReportCacheHit()
        {
            var hits = Interlocked.Increment(ref _cacheHitsCount);
            _cacheHits.WriteMetric((double)hits);
        }

        public void ReportCacheMiss()
        {
            var miss = Interlocked.Increment(ref _cacheMissCount);
            _cacheMisses.WriteMetric((double)miss);
        }
    }
}
