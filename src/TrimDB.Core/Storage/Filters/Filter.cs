using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;

namespace TrimDB.Core.Storage.Filters
{
    public class Filter
    {
        public bool MayContainKey(long hashedValue)
        {
            return true;
        }

        public void AddKey(ReadOnlySpan<byte> key)
        {

        }

        public int WriteToPipe(PipeWriter pipeWriter)
        {
            return 0;
        }
    }
}
