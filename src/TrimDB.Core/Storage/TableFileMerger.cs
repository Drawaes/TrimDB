using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrimDB.Core.InMemory;

namespace TrimDB.Core.Storage
{
    public class TableFileMerger : IAsyncEnumerator<IMemoryItem>, IAsyncDisposable
    {
        private IAsyncEnumerator<IMemoryItem>?[] _memoryItems;
        private IAsyncEnumerator<IMemoryItem>[] _initialList;
        private int _activeCount;
        private bool _hasInitialMove;
        private IAsyncEnumerator<IMemoryItem> _currentIterator;

        public TableFileMerger(IAsyncEnumerator<IMemoryItem>[] memoryItems)
        {
            _initialList = memoryItems;
            _memoryItems = new IAsyncEnumerator<IMemoryItem>?[memoryItems.Length];
            Array.Copy(memoryItems, _memoryItems, memoryItems.Length);
            _activeCount = memoryItems.Length;
            _currentIterator = _memoryItems[0]!;
        }

        public IMemoryItem Current => _currentIterator.Current;

        private void RemoveIterator(IAsyncEnumerator<IMemoryItem> exhausted)
        {
            for (var i = 0; i < _activeCount; i++)
            {
                if (_memoryItems[i] == exhausted)
                {
                    _activeCount--;
                    _memoryItems[i] = _memoryItems[_activeCount];
                    _memoryItems[_activeCount] = null;
                    return;
                }
            }
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            if (!_hasInitialMove)
            {
                _hasInitialMove = true;
                for (var i = 0; i < _activeCount; i++)
                {
                    await _memoryItems[i]!.MoveNextAsync();
                }
            }
            else
            {
                if (!await _currentIterator.MoveNextAsync())
                    RemoveIterator(_currentIterator);
            }

            if (_activeCount == 0) return false;

            _currentIterator = _memoryItems[0]!;
            for (var i = 1; i < _activeCount; i++)
            {
                var currentIterator = _memoryItems[i]!;
                var compare = _currentIterator.Current.Key.SequenceCompareTo(currentIterator.Current.Key);
                if (compare == 0)
                {
                    if (!await currentIterator.MoveNextAsync())
                    {
                        RemoveIterator(currentIterator);
                        i--; // re-visit the swapped-in element
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
