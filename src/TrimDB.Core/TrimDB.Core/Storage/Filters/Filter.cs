using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Storage.Filters
{
    public class Filter
    {
        public bool MayContainKey(long hashedValue)
        {
            return true;
        }
    }
}
