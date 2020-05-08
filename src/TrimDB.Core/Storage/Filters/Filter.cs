using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;

namespace TrimDB.Core.Storage.Filters
{
    public abstract class Filter
    {
        public abstract bool MayContainKey(long hashedValue);

        public abstract bool AddKey(ReadOnlySpan<byte> key);

        public abstract int WriteToPipe(PipeWriter pipeWriter);

        public abstract void LoadFromBlock(ReadOnlyMemory<byte> memory);
    }
}
