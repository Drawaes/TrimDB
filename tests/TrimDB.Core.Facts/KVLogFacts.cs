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
            KVLogManager kvLogManager = new FileBasedKVLogManager(tmpLogFile, tmpMetaFile);
            var valOffset = await kvLogManager.LogKV(Encoding.UTF8.GetBytes("answer"), Encoding.UTF8.GetBytes("42"), false);

            Assert.Equal(10, valOffset);

            await kvLogManager.RecordCommitted(valOffset);

            var val = await kvLogManager.ReadValueAtLocation(valOffset);
            var valStr = System.Text.Encoding.UTF8.GetString(val.ToArray(), 0, val.Length);
            Assert.Equal("42", valStr);

            // check no outstanding ops to apply and commit
            var allCommitted = await kvLogManager.IsAllCommitted();
            Assert.True(allCommitted);

            // log another
            var valOffset2 = await kvLogManager.LogKV(Encoding.UTF8.GetBytes("age"), Encoding.UTF8.GetBytes("23"), false);
            Assert.Equal(23, valOffset2);

            allCommitted = await kvLogManager.IsAllCommitted();
            Assert.False(allCommitted);

            await foreach(var o in kvLogManager.GetUncommittedOperations())
            {
                var k = System.Text.Encoding.UTF8.GetString(o.Key.ToArray(), 0, o.Key.Length);
                Assert.Equal("age", k);

                var v = System.Text.Encoding.UTF8.GetString(o.Value.ToArray(), 0, o.Value.Length);
                Assert.Equal("23", v);
            }
        }
    }
}
