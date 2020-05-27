using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;

namespace TrimDB.Core.Storage
{
    public class TableFileMerger : IAsyncEnumerator<IMemoryItem>
    {
        private IAsyncEnumerator<IMemoryItem>[] _memoryItems;
        private IAsyncEnumerator<IMemoryItem>[] _initialList;
        private bool _hasInitialMove;
        private IAsyncEnumerator<IMemoryItem> _currentIterator;

        public TableFileMerger(IAsyncEnumerator<IMemoryItem>[] memoryItems)
        {
            _initialList = memoryItems;
            _memoryItems = memoryItems;
            _currentIterator = _memoryItems[0];
        }

        public IMemoryItem Current => _currentIterator.Current;

        public async ValueTask<bool> MoveNextAsync()
        {
            if (!_hasInitialMove)
            {
                _hasInitialMove = true;
                foreach (var i in _memoryItems)
                {
                    await i.MoveNextAsync();
                }
            }
            else
            {
                if (!await _currentIterator.MoveNextAsync())
                    _memoryItems = _memoryItems.Where(mi => mi != _currentIterator).ToArray();
            }

            if (_memoryItems.Length == 0) return false;

            _currentIterator = _memoryItems[0];
            var iteratorCopy = _memoryItems;
            for (var i = 1; i < iteratorCopy.Length; i++)
            {
                var currentIterator = iteratorCopy[i];
                var compare = _currentIterator.Current.Key.SequenceCompareTo(currentIterator.Current.Key);
                if (compare == 0)
                {
                    if (!await currentIterator.MoveNextAsync())
                    {
                        if (_memoryItems.Contains(currentIterator))
                        {
                            _memoryItems = _memoryItems.Where(mi => mi != currentIterator).ToArray();
                        }
                    }
                    continue;
                }
                if (compare > 0)
                {
                    _currentIterator = currentIterator;
                }
            }

            return true;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var i in _initialList)
            {
                await i.DisposeAsync();
            }
        }
    }
}
