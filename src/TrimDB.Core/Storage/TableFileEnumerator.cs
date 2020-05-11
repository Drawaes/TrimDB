using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;
using TrimDB.Core.Storage.Blocks;

namespace TrimDB.Core.Storage
{
    internal class TableFileEnumerator : IAsyncEnumerator<IMemoryItem>
    {
        private readonly TableFile _tableFile;
        private int _blockNumber = -1;
        private BlockReader _blockReader;
        private readonly ReusableMemoryItem _memItem = new ReusableMemoryItem();

        public TableFileEnumerator(TableFile tableFile)
        {
            _tableFile = tableFile;
        }

        public IMemoryItem Current => _memItem;

        public ValueTask DisposeAsync()
        {
            _blockReader?.Dispose();
            _tableFile.ReleaseIterator();
            return default;
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            if (_blockNumber == -1)
            {
                _blockNumber = 0;
                _blockReader = await _tableFile.GetKVBlock(_blockNumber);
            }

            if (_blockReader.TryGetNextKey(out _))
            {
                _memItem.CurrentBlock = _blockReader;
                return true;
            }

            _blockNumber++;
            if (_blockNumber >= _tableFile.BlockCount) return false;

            _blockReader?.Dispose();
            _blockReader = await _tableFile.GetKVBlock(_blockNumber);
            if (_blockReader == null)
            {
                return false;
            }

            if (!_blockReader.TryGetNextKey(out _))
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

            public ReadOnlySpan<byte> Value => CurrentBlock.GetCurrentValue().Span;

            public bool IsDeleted => throw new NotImplementedException();
        }
    }
}
