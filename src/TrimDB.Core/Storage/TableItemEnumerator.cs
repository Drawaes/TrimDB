using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage
{
    public class TableItemEnumerator : IEnumerator<TableItem>
    {
        public TableItemEnumerator(TableFile table)
        {

        }

        public TableItem Current => throw new NotImplementedException();

        object? IEnumerator.Current => throw new NotImplementedException();

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public bool MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }
    }

    public struct TableItem
    {

    }
}
