using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NuGet.Frameworks;
using TrimDB.Core.InMemory.SkipList32;
using TrimDB.Core.InMemory.SkipList64;
using Xunit;
using TrimDB.Core.KVLog;
using System.Threading;
using System.IO;

namespace TrimDB.Core.Facts
{
    public class KVLogFacts
    {
        [Fact]
        public async Task TestBasicKVLogOperations()
        {
            var tmpLogFile = System.IO.Path.GetTempFileName();
            var tmpMetaFile = System.IO.Path.GetTempFileName();
            var kvLogManager = new KVLogManager(tmpLogFile, tmpMetaFile);
            var c = kvLogManager.GetChannelWriter();
            var po = new PutOperation();
            long off = 0;
            po.Key = Encoding.UTF8.GetBytes("answer");
            po.Value = Encoding.UTF8.GetBytes("42");
            po.Completed = (long voffset) =>
            {
                off = voffset;
            };

            await c.WriteAsync(po);

            while (off == 0)
            {
                Thread.SpinWait(10);
            }

            Assert.Equal(10, off);

            await kvLogManager.RecordCommitted(10);
        }
    }
}
