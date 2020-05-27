using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TrimDB.DatabaseStress
{
    public class KeyValueGenerator
    {
        private Random[] _threadRandoms;

        public KeyValueGenerator(int numberOfThreads, int seed)
        {
            var mainRandom = new Random(seed);
            _threadRandoms = new Random[numberOfThreads];

            for (var i = 0; i < _threadRandoms.Length; i++)
            {
                _threadRandoms[i] = new Random(mainRandom.Next());
            }
        }

        public void GetKeyValue(Span<byte> key, Span<byte> value, short threadId, int iteration)
        {
            var keyRandomSize = key.Length - (sizeof(short) + sizeof(int));
            _threadRandoms[threadId].NextBytes(key.Slice(0, keyRandomSize));
            _threadRandoms[threadId].NextBytes(value);

            System.Buffers.Binary.BinaryPrimitives.WriteInt16LittleEndian(key.Slice(keyRandomSize), threadId);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(key.Slice(keyRandomSize + sizeof(short)), iteration);
        }
    }
}
