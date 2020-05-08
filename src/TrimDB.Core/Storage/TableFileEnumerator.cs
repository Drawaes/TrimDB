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

        object? IEnumerator.Current => throw new NotImplementedException();

        public void Dispose()
        {
            _tableFile.ReleaseIterator();
        }

        public ValueTask DisposeAsync()
        {
            throw new NotImplementedException();
        }

        public bool MoveNext()
        {
            if(_blockReader == null)
            {
                _blockNumber++;
                _blockReader = _tableFile.GetKVBlock(_blockNumber);
            }


        }

        public ValueTask<bool> MoveNextAsync()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            _blockNumber = -1;
        }
    }
}
