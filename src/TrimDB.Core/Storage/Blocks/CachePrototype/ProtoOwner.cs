using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage.Blocks.CachePrototype
{
    public class ProtoOwner : MemoryManager<byte>
    {
        private readonly byte[] _array;
        private readonly int _offset;
        private readonly ProtoLRUCache _parent;
        private readonly BlockIdentifier _bid;

        internal ProtoOwner(byte[] array, int offset, ProtoLRUCache parent, BlockIdentifier bid)
        {
            _array = array;
            _offset = offset;
            _parent = parent;
            _bid = bid;
        }

        public override Span<byte> GetSpan() => new Span<byte>(_array, _offset, FileConsts.PageSize);

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            throw new NotImplementedException();
        }

        public override void Unpin()
        {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing) => _parent.ReturnReference(_bid);
    }
}
