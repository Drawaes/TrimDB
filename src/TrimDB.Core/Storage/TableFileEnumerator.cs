using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;

namespace TrimDB.Core.Storage
{
    internal class TableFileEnumerator : IAsyncEnumerator<IMemoryItem>
    {
        private TableFile _tableFile;
        private int _blockNumber = -1;
        private BlockReader _blockReader;
        private ReusableMemoryItem _memItem = new ReusableMemoryItem();

        public TableFileEnumerator(TableFile tableFile)
        {
            _tableFile = tableFile;
        }

        public IMemoryItem Current => _memItem;

        public ValueTask DisposeAsync()
        {
            _tableFile.ReleaseIterator();
            return default;
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            if (_blockReader == null)
            {
                _blockNumber++;
                _blockReader = await _tableFile.GetKVBlock(_blockNumber);
            }

            if (_blockReader.TryGetNextKey(out _))
            {
                _memItem.CurrentBlock = _blockReader;
                return true;
            }

            _blockNumber++;
            _blockReader = await _tableFile.GetKVBlock(_blockNumber);
            if (_blockReader == null)
            {
                return false;
            }
            _memItem.CurrentBlock = _blockReader;
            return true;
        }

        internal class ReusableMemoryItem : IMemoryItem
        {
            public BlockReader CurrentBlock { get; set; }

            public ReadOnlySpan<byte> Key => CurrentBlock.GetCurrentKey();

            public ReadOnlySpan<byte> Value => throw new NotImplementedException();

            public bool IsDeleted => throw new NotImplementedException();
        }
    }
}
