using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Facts
{
    internal class CommonData
    {
        public static string[] Words => System.IO.File.ReadAllLines("words.txt");
    }
}
