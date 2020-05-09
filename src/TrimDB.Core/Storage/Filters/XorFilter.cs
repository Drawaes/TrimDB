using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using TrimDB.Core.Hashing;

namespace TrimDB.Core.Storage.Filters
{
    public class XorFilter : Filter
    {
        private const int BitsPerFingerPrint = 8;
        private const int Hashes = 3;
        private const byte HeaderValue = 1;
        private const int SizeFactor = 123;

        private int _size;
        private int _arrayLength;
        private int _blockLength;
        private long _seed;
        private byte[] _fingerPrints;
        private List<long> _keys = new List<long>();
        private MurmurHash3 _hash = new MurmurHash3();

        private static int GetArrayLength(int size) => Hashes + (SizeFactor * size / 100);

        public void LoadFromKeys(long[] keys)
        {
            _size = keys.Length;
            _arrayLength = GetArrayLength(_size);
            _blockLength = _arrayLength / Hashes;
            var m = _arrayLength;
            var reverseOrder = new long[_size];
            var reverseH = new byte[_size];

            ulong rngCounter = 1;

            int reverseOrderPos;
            var t2count = new byte[m];
            var t2 = new long[m];
            do
            {
                _seed = (long)SplitMix(ref rngCounter);
                Array.Fill(t2count, (byte)0);
                Array.Fill(t2, 0);
                foreach (var k in keys)
                {
                    for (int hi = 0; hi < Hashes; hi++)
                    {
                        int h = GetHash(k, _seed, hi);
                        t2[h] ^= k;
                        if (t2count[h] > 120)
                        {
                            // probably something wrong with the hash function
                            throw new InvalidDataException();
                        }
                        t2count[h]++;
                    }
                }
                reverseOrderPos = 0;
                var alone = new int[Hashes, _blockLength];
                var alonePos = new int[Hashes];
                for (var nextAlone = 0; nextAlone < Hashes; nextAlone++)
                {
                    for (var i = 0; i < _blockLength; i++)
                    {
                        if (t2count[nextAlone * _blockLength + i] == 1)
                        {
                            alone[nextAlone, alonePos[nextAlone]++] = nextAlone * _blockLength + i;
                        }
                    }
                }
                int found = -1;
                while (true)
                {
                    int i = -1;
                    for (int hi = 0; hi < Hashes; hi++)
                    {
                        if (alonePos[hi] > 0)
                        {
                            i = alone[hi, --alonePos[hi]];
                            found = hi;
                            break;
                        }
                    }
                    if (i == -1)
                    {
                        // no entry found
                        break;
                    }
                    if (t2count[i] <= 0)
                    {
                        continue;
                    }
                    long k = t2[i];
                    if (t2count[i] != 1)
                    {
                        throw new Exception();
                    }
                    --t2count[i];
                    for (var hi = 0; hi < Hashes; hi++)
                    {
                        if (hi != found)
                        {
                            int h = GetHash(k, _seed, hi);
                            int newCount = --t2count[h];
                            if (newCount == 1)
                            {
                                alone[hi, alonePos[hi]++] = h;
                            }
                            t2[h] ^= k;
                        }
                    }
                    reverseOrder[reverseOrderPos] = k;
                    reverseH[reverseOrderPos] = (byte)found;
                    reverseOrderPos++;
                }

            } while (reverseOrderPos != _size);

            var fp = new byte[m];
            for (int i = reverseOrderPos - 1; i >= 0; i--)
            {
                long k = reverseOrder[i];
                int found = reverseH[i];
                int change = -1;
                long hash = (long)MixSplit((ulong)k, (ulong)_seed);
                int xor = FingerPrint(hash);
                for (int hi = 0; hi < Hashes; hi++)
                {
                    int h = GetHash(k, _seed, hi);
                    if (found == hi)
                    {
                        change = h;
                    }
                    else
                    {
                        xor ^= fp[h];
                    }
                }
                fp[change] = (byte)xor;
            }
            _fingerPrints = fp;
        }

        public override bool AddKey(ReadOnlySpan<byte> key)
        {
            var hash = _hash.ComputeHash64(key);
            _keys.Add((long)hash);
            return true;
        }

        public override bool MayContainKey(long key)
        {
            long hash = (long)MixSplit((ulong)key, (ulong)_seed);
            int f = FingerPrint(hash);
            var r0 = (uint)hash;
            var r1 = (uint)System.Numerics.BitOperations.RotateLeft((ulong)hash, 21);
            var r2 = (uint)System.Numerics.BitOperations.RotateLeft((ulong)hash, 42);
            var h0 = Reduce(r0, (uint)_blockLength);
            var h1 = Reduce(r1, (uint)_blockLength) + _blockLength;
            var h2 = Reduce(r2, (uint)_blockLength) + 2 * _blockLength;
            f ^= _fingerPrints[h0] ^ _fingerPrints[h1] ^ _fingerPrints[h2];
            return (f & 0xff) == 0;
        }

        public override int WriteToPipe(PipeWriter pipeWriter)
        {
            var uniq = _keys.Distinct().ToArray();
            var coll = (uniq.Count() - _keys.Count) / (double)_keys.Count;
            Console.WriteLine($"We had {coll * 100}% keys smash into each other badly :) ");

            LoadFromKeys(uniq);
            var sizeToSave = sizeof(byte) + sizeof(int) + sizeof(long) + _fingerPrints.Length;
            var span = pipeWriter.GetSpan(sizeToSave);

            span[0] = HeaderValue;
            span = span.Slice(1);
            BinaryPrimitives.WriteInt64LittleEndian(span, _seed);
            span = span.Slice(sizeof(long));
            BinaryPrimitives.WriteInt32LittleEndian(span, _blockLength);
            span = span.Slice(sizeof(int));
            _fingerPrints.AsSpan().CopyTo(span);
            pipeWriter.Advance(sizeToSave);
            return sizeToSave;
        }

        public override void LoadFromBlock(ReadOnlyMemory<byte> memory)
        {
            var data = memory.Span;
            var header = data[0];
            if (header != HeaderValue)
            {
                throw new InvalidDataException("The header for the XOR filter is incorrect");
            }
            data = data.Slice(1);
            _seed = BinaryPrimitives.ReadInt64LittleEndian(data);
            data = data.Slice(sizeof(long));
            _blockLength = BinaryPrimitives.ReadInt32LittleEndian(data);
            data = data.Slice(sizeof(int));
            _fingerPrints = new byte[data.Length];
            data.CopyTo(_fingerPrints);
        }

        private static ulong MixSplit(ulong key, ulong seed)
        {
            unchecked
            {
                return MurMur64(key + seed);
            }
        }

        private int FingerPrint(long hash) => (int)(hash & ((1 << BitsPerFingerPrint) - 1));

        private static uint Reduce(uint hash, uint n)
        {
            // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
            return (uint)(((ulong)hash * (ulong)n) >> 32);
        }

        private int GetHash(long key, long seed, int index)
        {
            unchecked
            {
                long r = (long)System.Numerics.BitOperations.RotateLeft(MixSplit((ulong)key, (ulong)seed), (byte)(21 * index));
                r = Reduce((uint)r, (uint)_blockLength);
                r += index * _blockLength;
                return (int)r;
            }
        }

        public static ulong MurMur64(ulong h)
        {
            unchecked
            {
                h ^= h >> 33;
                h *= 0xff51afd7ed558ccd;
                h ^= h >> 33;
                h *= 0xc4ceb9fe1a85ec53;
                h ^= h >> 33;
                return h;
            }
        }

        private static ulong SplitMix(ref ulong seed)
        {
            unchecked
            {
                seed = seed + 0x9e3779B97F4A7C15;
                var z = seed;
                z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9;
                z = (z ^ (z >> 27)) * 0x94D049BB133111EB;
                return z ^ (z >> 31);
            }
        }
    }
}
