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
            var valOffset = await kvLogManager.LogKV(Encoding.UTF8.GetBytes("answer"), Encoding.UTF8.GetBytes("42"), false);

            Assert.Equal(10, valOffset);

            await kvLogManager.RecordCommitted(valOffset);

            var val = await kvLogManager.ReadValueAtLocation(valOffset);
            var valStr = System.Text.Encoding.UTF8.GetString(val.ToArray(), 0, val.Length);
            Assert.Equal("42", valStr);

            // check state returns no operations

            // store new kv

            // check state returns one operation

        }
    }
}
