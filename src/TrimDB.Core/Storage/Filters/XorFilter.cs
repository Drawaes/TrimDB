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
        private const int BitsPerFingerPrint = 16;
        private const int Hashes = 3;
        private const byte HeaderValue8 = 1;   // Legacy 8-bit format
        private const byte HeaderValue16 = 2;  // New 16-bit format
        private const int SizeFactor = 123;

        private int _size;
        private int _arrayLength;
        private int _blockLength;
        private long _seed;
        private ushort[] _fingerPrints = null!;
        private bool _is8Bit; // true when loaded from legacy 8-bit format
        private readonly HashSet<long> _keys;
        private readonly MurmurHash3 _hash = new MurmurHash3();
        private readonly bool _useMurMur;

        public XorFilter(int approxSize, bool useMurMur)
        {
            _useMurMur = useMurMur;
            if (approxSize < 1)
            {
                _keys = new HashSet<long>();
            }
            else
            {
                _keys = new HashSet<long>(approxSize);
            }
        }

        private static int GetArrayLength(int size) => Math.Max(Hashes * 12, Hashes + (SizeFactor * size / 100));

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
                    for (var hi = 0; hi < Hashes; hi++)
                    {
                        var h = GetHash(k, _seed, hi);
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
                var found = -1;
                while (true)
                {
                    var i = -1;
                    for (var hi = 0; hi < Hashes; hi++)
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
                    var k = t2[i];
                    if (t2count[i] != 1)
                    {
                        throw new Exception();
                    }
                    --t2count[i];
                    for (var hi = 0; hi < Hashes; hi++)
                    {
                        if (hi != found)
                        {
                            var h = GetHash(k, _seed, hi);
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

            var fp = new ushort[m];
            for (var i = reverseOrderPos - 1; i >= 0; i--)
            {
                var k = reverseOrder[i];
                var change = -1;
                var hash = (long)MixSplit((ulong)k, (ulong)_seed);
                var xor = (ushort)FingerPrint(hash);
                for (var hi = 0; hi < Hashes; hi++)
                {
                    var h = GetHash(k, _seed, hi);
                    int foundIdx = reverseH[i];
                    if (foundIdx == hi)
                    {
                        change = h;
                    }
                    else
                    {
                        xor ^= fp[h];
                    }
                }
                fp[change] = xor;
            }
            _fingerPrints = fp;
            _is8Bit = false;
        }

        public override bool AddKey(ReadOnlySpan<byte> key)
        {
            ulong hash;
            if (_useMurMur)
            {
                hash = _hash.ComputeHash64(key);
            }
            else
            {
                hash = Farmhash.Sharp.Farmhash.Hash64(key);
            }
            _keys.Add((long)hash);
            return true; // HashSet.Add deduplicates at insertion time
        }

        public override bool MayContainKey(long key)
        {
            var hash = (long)MixSplit((ulong)key, (ulong)_seed);
            var f = FingerPrint(hash);
            var r0 = (uint)hash;
            var r1 = (uint)System.Numerics.BitOperations.RotateLeft((ulong)hash, 21);
            var r2 = (uint)System.Numerics.BitOperations.RotateLeft((ulong)hash, 42);
            var h0 = Reduce(r0, (uint)_blockLength);
            var h1 = Reduce(r1, (uint)_blockLength) + _blockLength;
            var h2 = Reduce(r2, (uint)_blockLength) + 2 * _blockLength;
            f ^= _fingerPrints[h0] ^ _fingerPrints[h1] ^ _fingerPrints[h2];
            var mask = _is8Bit ? 0xff : 0xffff;
            return (f & mask) == 0;
        }

        public override int WriteToPipe(PipeWriter pipeWriter)
        {
            var uniq = _keys.ToArray();

            LoadFromKeys(uniq);
            var fingerprintBytes = _fingerPrints.Length * sizeof(ushort);
            var sizeToSave = sizeof(byte) + sizeof(long) + sizeof(int) + fingerprintBytes;
            var span = pipeWriter.GetSpan(sizeToSave);

            span[0] = HeaderValue16;
            span = span.Slice(1);
            BinaryPrimitives.WriteInt64LittleEndian(span, _seed);
            span = span.Slice(sizeof(long));
            BinaryPrimitives.WriteInt32LittleEndian(span, _blockLength);
            span = span.Slice(sizeof(int));
            // Write ushort[] as little-endian bytes
            for (var i = 0; i < _fingerPrints.Length; i++)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(span[(i * sizeof(ushort))..], _fingerPrints[i]);
            }
            pipeWriter.Advance(sizeToSave);
            return sizeToSave;
        }

        public override void LoadFromBlock(ReadOnlyMemory<byte> memory)
        {
            var data = memory.Span;
            var header = data[0];
            if (header == HeaderValue8)
            {
                LoadFromBlock8Bit(data.Slice(1));
            }
            else if (header == HeaderValue16)
            {
                LoadFromBlock16Bit(data.Slice(1));
            }
            else
            {
                throw new InvalidDataException("The header for the XOR filter is incorrect");
            }
        }

        private void LoadFromBlock8Bit(ReadOnlySpan<byte> data)
        {
            _is8Bit = true;
            _seed = BinaryPrimitives.ReadInt64LittleEndian(data);
            data = data.Slice(sizeof(long));
            _blockLength = BinaryPrimitives.ReadInt32LittleEndian(data);
            data = data.Slice(sizeof(int));
            // Promote 8-bit fingerprints to 16-bit array
            _fingerPrints = new ushort[data.Length];
            for (var i = 0; i < data.Length; i++)
            {
                _fingerPrints[i] = data[i];
            }
        }

        private void LoadFromBlock16Bit(ReadOnlySpan<byte> data)
        {
            _is8Bit = false;
            _seed = BinaryPrimitives.ReadInt64LittleEndian(data);
            data = data.Slice(sizeof(long));
            _blockLength = BinaryPrimitives.ReadInt32LittleEndian(data);
            data = data.Slice(sizeof(int));
            _fingerPrints = new ushort[data.Length / sizeof(ushort)];
            for (var i = 0; i < _fingerPrints.Length; i++)
            {
                _fingerPrints[i] = BinaryPrimitives.ReadUInt16LittleEndian(data[(i * sizeof(ushort))..]);
            }
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
                var r = (long)System.Numerics.BitOperations.RotateLeft(MixSplit((ulong)key, (ulong)seed), (byte)(21 * index));
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
                seed += 0x9e3779B97F4A7C15;
                var z = seed;
                z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9;
                z = (z ^ (z >> 27)) * 0x94D049BB133111EB;
                return z ^ (z >> 31);
            }
        }
    }
}
