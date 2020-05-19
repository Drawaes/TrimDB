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
        private IncrementingEventCounter _cacheHits;
        private IncrementingEventCounter _cacheMisses;
        private int _cacheHitsCount;
        private int _cacheMissCount;
        public static readonly ProtoEventSource Log = new ProtoEventSource();

        public ProtoEventSource()
        {
            _cacheHits = new IncrementingEventCounter("CacheHits", this);
            _cacheMisses = new IncrementingEventCounter("CacheMisses", this);
        }

        public void ReportCacheHit()
        {
            _cacheHits.Increment();
        }

        public void ReportCacheMiss()
        {
            _cacheMisses.Increment();
        }
    }
}
