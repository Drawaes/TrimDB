using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TrimDB.Core.InMemory;

namespace TrimDB.Core.Storage
{
    public class TableFileMerger : IAsyncEnumerator<IMemoryItem>
    {
        private IEnumerator<IMemoryItem>[] _memoryItems;
        private bool _hasInitialMove;
        private IEnumerator<IMemoryItem> _currentIterator;

        public TableFileMerger(IEnumerator<IMemoryItem>[] memoryItems)
        {
            _memoryItems = memoryItems;
            _currentIterator = _memoryItems[0];
        }

        public IMemoryItem Current => _currentIterator.Current;

        object? IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (!_hasInitialMove)
            {
                _hasInitialMove = true;
                foreach (var i in _memoryItems)
                {
                    i.MoveNext();
                }
            }
            else
            {
                if (!_currentIterator.MoveNext())
                    _memoryItems = _memoryItems.Where(mi => mi != _currentIterator).ToArray();
            }

            if (_memoryItems.Length == 0) return false;

            _currentIterator = _memoryItems[0];
            var iteratorCopy = _memoryItems;
            for (var i = 1; i < iteratorCopy.Length; i++)
            {
                var currentIterator = _memoryItems[i];
                var compare = _currentIterator.Current.Key.SequenceCompareTo(currentIterator.Current.Key);
                if (compare == 0)
                {
                    if (!currentIterator.MoveNext())
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

        public void Reset()
        {
            foreach (var i in _memoryItems)
            {
                i.Dispose();
            }
        }

        public void Dispose()
        {
            foreach (var i in _memoryItems)
            {
                i.Dispose();
            }
        }
    }
}
