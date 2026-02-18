using System.Collections.Generic;

namespace TrimDB.Nethermind.Interfaces
{
    public interface IDbMeta
    {
        void Flush();
        void Compact();
        void Clear();
        IEnumerable<DbMetric> GetMetrics();
        long GetSize();
    }
}
