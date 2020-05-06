using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TrimDB.Core.InMemory.SkipList64
{
    public class SkipListHeightGenerator64
    {
        private static readonly Random s_global = new Random();

        [ThreadStatic]
        private static Random? s_local;

        private static uint Next()
        {
            var inst = s_local;
            if (inst == null)
            {
                int seed;
                lock (s_global)
                {
                    seed = s_global.Next();
                }
                s_local = inst = new Random(seed);
            }
            return (uint)inst.Next();
        }

        private readonly int _maxHeight;
        private int _currentHeight = 1;

        public SkipListHeightGenerator64(byte maxHeight)
        {
            if (maxHeight > 32) throw new ArgumentOutOfRangeException(nameof(maxHeight), "The max height cannot be larger than 32");
            _maxHeight = maxHeight;
        }

        public byte MaxHeight => (byte)_maxHeight;

        public byte CurrentHeight => (byte)_currentHeight;

        public byte GetHeight()
        {
            var randomNumber = Next();
            var leadingZeros = System.Runtime.Intrinsics.X86.Lzcnt.LeadingZeroCount(randomNumber) + 1;
            var maxValue = Math.Min(_maxHeight, _currentHeight + 1);

            var height = (byte)Math.Min(leadingZeros, maxValue);

            while (true)
            {
                var oldHeight = _currentHeight;
                if (height > oldHeight)
                {
                    if (Interlocked.CompareExchange(ref _currentHeight, height, oldHeight) != oldHeight) continue;
                }
                return height;
            }
        }
    }
}
