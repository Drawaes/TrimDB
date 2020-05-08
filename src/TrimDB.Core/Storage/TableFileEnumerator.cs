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

        public TableFileEnumerator(TableFile tableFile)
        {
            _tableFile = tableFile;
        }

        public IMemoryItem Current => throw new NotImplementedException();

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
                return true;
            }

            _blockNumber++;
            _blockReader = await _tableFile.GetKVBlock(_blockNumber);
            if (_blockReader == null)
            {
                return false;
            }
            return true;
        }
    }
}
