using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
            await using var kvLogManager = new FileBasedKVLogManager(tmpLogFile);

            // LogKV returns the entry-start offset (0 for first entry in empty file)
            var entryStart = await kvLogManager.LogKV(Encoding.UTF8.GetBytes("answer"), Encoding.UTF8.GetBytes("42"), false);
            Assert.Equal(0, entryStart);

            // Commit everything written so far using CurrentPosition (entry-end)
            await kvLogManager.RecordCommitted(kvLogManager.CurrentPosition);

            var val = await kvLogManager.ReadValueAtLocation(entryStart);
            var valStr = Encoding.UTF8.GetString(val.ToArray(), 0, val.Length);
            Assert.Equal("42", valStr);

            // check no outstanding ops to apply and commit
            var allCommitted = await kvLogManager.IsAllCommitted();
            Assert.True(allCommitted);

            // log another — entry-start = end of first entry + commit marker (12 bytes)
            // Entry 1: [keyLen:4][key:6="answer"][del:1][valLen:4][val:2="42"] = 17 bytes
            // Commit marker: [sentinel:4=-2][offset:8] = 12 bytes
            var entryStart2 = await kvLogManager.LogKV(Encoding.UTF8.GetBytes("age"), Encoding.UTF8.GetBytes("23"), false);
            Assert.Equal(29, entryStart2); // 17 (entry) + 12 (commit marker)

            allCommitted = await kvLogManager.IsAllCommitted();
            Assert.False(allCommitted);

            await foreach(var o in kvLogManager.GetUncommittedOperations())
            {
                var k = Encoding.UTF8.GetString(o.Key.ToArray(), 0, o.Key.Length);
                Assert.Equal("age", k);

                var v = Encoding.UTF8.GetString(o.Value.ToArray(), 0, o.Value.Length);
                Assert.Equal("23", v);
            }
        }
    }
}
