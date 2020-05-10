using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace TrimDB.Core.Hashing
{
    public interface IHashFunction
    {

        ulong ComputeHash64(in ReadOnlySpan<byte> buffer);
    }


    /// <summary>
    /// <see href="https://github.com/aappleby/smhasher/wiki/MurmurHash3">Docs</see>
    /// and <see href="https://github.com/aappleby/smhasher/blob/92cf3702fcfaadc84eb7bef59825a23e0cd84f56/src/MurmurHash3.cpp#L255">MurmurHash3.cpp</see>
    /// You can do this online on <seealso href="http://murmurhash.shorelabs.com/"/>
    /// Contains x64-optimized versions only.
    /// </summary>
    public class MurmurHash3 : IHashFunction
    {

        /// <inheritdoc/>
        public unsafe uint ComputeHash32(in ReadOnlySpan<byte> buffer)
        {
            const uint Seed = 0;
            var len = buffer.Length;
            var nblocks = len / 16;
            var h1 = Seed;

            const uint C1 = 0xcc9e2d51;
            const uint C2 = 0x1b873593;

            // body
            fixed (byte* pbuffer = buffer)
            {
                var pinput = pbuffer;
                var body = (uint*)pinput;
                uint k1;

                for (var i = -nblocks; i > 0; i++)
                {
                    k1 = body[i];

                    k1 *= C1;
                    k1 = (k1 << 15) | (k1 >> (32 - 15)); // ROTL32(k1, 15)
                    k1 *= C2;

                    h1 ^= k1;
                    h1 = (h1 << 13) | (h1 >> (32 - 13)); // ROTL32(h1, 13)
                    h1 = (h1 * 5) + 0xe6546b64;
                }

                // tail
                var tail = pinput + (nblocks * 4);
                k1 = 0;

                switch (len & 3)
                {
                    case 3:
                        k1 ^= (uint)tail[2] << 16;
                        goto case 2;
                    case 2:
                        k1 ^= (uint)tail[1] << 8;
                        goto case 1;
                    case 1:
                        k1 ^= (uint)tail[0];
                        k1 *= C1;
                        k1 = (k1 << 15) | (k1 >> (32 - 15)); // ROTL32(k1, 15)
                        k1 *= C2;
                        h1 ^= k1;
                        break;
                };
            }
            // finalization

            h1 ^= (uint)len;
            h1 = FMix32(h1);

            return h1;
        }


        /// <inheritdoc/>
        public unsafe byte[] ComputeHash128(in ReadOnlySpan<byte> buffer)
        {
            const ulong C1 = 0x87c37b91_114253d5;
            const ulong C2 = 0x4cf5ad43_2745937f;
            const ulong Seed = 0;
            var len = buffer.Length;
            var nblocks = len / 16;

            var h1 = Seed;
            var h2 = Seed;

            // body
            fixed (byte* pbuffer = buffer)
            {
                var pinput = pbuffer;
                var body = (ulong*)pinput;

                ulong k1;
                ulong k2;

                for (var i = 0; i < nblocks; i++)
                {
                    k1 = body[i * 2];
                    k2 = body[(i * 2) + 1];

                    k1 *= C1;
                    k1 = (k1 << 31) | (k1 >> (64 - 31)); // ROTL64(k1, 31);
                    k1 *= C2;
                    h1 ^= k1;

                    h1 = (h1 << 27) | (h1 >> (64 - 27)); // ROTL64(h1, 27);
                    h1 += h2;
                    h1 = (h1 * 5) + 0x52dce729;

                    k2 *= C2;
                    k2 = (k2 << 33) | (k2 >> (64 - 33)); // ROTL64(k2, 33);
                    k2 *= C1;
                    h2 ^= k2;

                    h2 = (h2 << 31) | (h2 >> (64 - 31)); // ROTL64(h2, 31);
                    h2 += h1;
                    h2 = (h2 * 5) + 0x38495ab5;
                }

                // tail

                k1 = 0;
                k2 = 0;

                var tail = pinput + (nblocks * 16);
                switch (len & 15)
                {
                    case 15:
                        k2 ^= (ulong)tail[14] << 48;
                        goto case 14;
                    case 14:
                        k2 ^= (ulong)tail[13] << 40;
                        goto case 13;
                    case 13:
                        k2 ^= (ulong)tail[12] << 32;
                        goto case 12;
                    case 12:
                        k2 ^= (ulong)tail[11] << 24;
                        goto case 11;
                    case 11:
                        k2 ^= (ulong)tail[10] << 16;
                        goto case 10;
                    case 10:
                        k2 ^= (ulong)tail[9] << 8;
                        goto case 9;
                    case 9:
                        k2 ^= tail[8];
                        k2 *= C2;
                        k2 = (k2 << 33) | (k2 >> (64 - 33)); // ROTL64(k2, 33);
                        k2 *= C1;
                        h2 ^= k2;
                        goto case 8;
                    case 8:
                        k1 ^= (ulong)tail[7] << 56;
                        goto case 7;
                    case 7:
                        k1 ^= (ulong)tail[6] << 48;
                        goto case 6;
                    case 6:
                        k1 ^= (ulong)tail[5] << 40;
                        goto case 5;
                    case 5:
                        k1 ^= (ulong)tail[4] << 32;
                        goto case 4;
                    case 4:
                        k1 ^= (ulong)tail[3] << 24;
                        goto case 3;
                    case 3:
                        k1 ^= (ulong)tail[2] << 16;
                        goto case 2;
                    case 2:
                        k1 ^= (ulong)tail[1] << 8;
                        goto case 1;
                    case 1:
                        k1 ^= tail[0];
                        k1 *= C1;
                        k1 = (k1 << 31) | (k1 >> (64 - 31)); // ROTL64(k1, 31);
                        k1 *= C2;
                        h1 ^= k1;
                        break;
                }
            }

            // finalization
            h1 ^= (ulong)len;
            h2 ^= (ulong)len;

            h1 += h2;
            h2 += h1;

            h1 = FMix64(h1);
            h2 = FMix64(h2);

            h1 += h2;
            h2 += h1;

            var ret = new byte[16];
            fixed (byte* pret = ret)
            {
                var ulpret = (ulong*)pret;

                ulpret[0] = Reverse(h1);
                ulpret[1] = Reverse(h2);
            }
            return ret;
        }

        public unsafe ulong ComputeHash64(in ReadOnlySpan<byte> buffer)
        {
            const ulong C1 = 0x87c37b91_114253d5;
            const ulong C2 = 0x4cf5ad43_2745937f;
            const ulong Seed = 0;
            var len = buffer.Length;
            var nblocks = len / 16;

            var h1 = Seed;
            var h2 = Seed;

            // body
            fixed (byte* pbuffer = buffer)
            {
                var pinput = pbuffer;
                var body = (ulong*)pinput;

                ulong k1;
                ulong k2;

                for (var i = 0; i < nblocks; i++)
                {
                    k1 = body[i * 2];
                    k2 = body[(i * 2) + 1];

                    k1 *= C1;
                    k1 = (k1 << 31) | (k1 >> (64 - 31)); // ROTL64(k1, 31);
                    k1 *= C2;
                    h1 ^= k1;

                    h1 = (h1 << 27) | (h1 >> (64 - 27)); // ROTL64(h1, 27);
                    h1 += h2;
                    h1 = (h1 * 5) + 0x52dce729;

                    k2 *= C2;
                    k2 = (k2 << 33) | (k2 >> (64 - 33)); // ROTL64(k2, 33);
                    k2 *= C1;
                    h2 ^= k2;

                    h2 = (h2 << 31) | (h2 >> (64 - 31)); // ROTL64(h2, 31);
                    h2 += h1;
                    h2 = (h2 * 5) + 0x38495ab5;
                }

                // tail

                k1 = 0;
                k2 = 0;

                var tail = pinput + (nblocks * 16);
                switch (len & 15)
                {
                    case 15:
                        k2 ^= (ulong)tail[14] << 48;
                        goto case 14;
                    case 14:
                        k2 ^= (ulong)tail[13] << 40;
                        goto case 13;
                    case 13:
                        k2 ^= (ulong)tail[12] << 32;
                        goto case 12;
                    case 12:
                        k2 ^= (ulong)tail[11] << 24;
                        goto case 11;
                    case 11:
                        k2 ^= (ulong)tail[10] << 16;
                        goto case 10;
                    case 10:
                        k2 ^= (ulong)tail[9] << 8;
                        goto case 9;
                    case 9:
                        k2 ^= tail[8];
                        k2 *= C2;
                        k2 = (k2 << 33) | (k2 >> (64 - 33)); // ROTL64(k2, 33);
                        k2 *= C1;
                        h2 ^= k2;
                        goto case 8;
                    case 8:
                        k1 ^= (ulong)tail[7] << 56;
                        goto case 7;
                    case 7:
                        k1 ^= (ulong)tail[6] << 48;
                        goto case 6;
                    case 6:
                        k1 ^= (ulong)tail[5] << 40;
                        goto case 5;
                    case 5:
                        k1 ^= (ulong)tail[4] << 32;
                        goto case 4;
                    case 4:
                        k1 ^= (ulong)tail[3] << 24;
                        goto case 3;
                    case 3:
                        k1 ^= (ulong)tail[2] << 16;
                        goto case 2;
                    case 2:
                        k1 ^= (ulong)tail[1] << 8;
                        goto case 1;
                    case 1:
                        k1 ^= tail[0];
                        k1 *= C1;
                        k1 = (k1 << 31) | (k1 >> (64 - 31)); // ROTL64(k1, 31);
                        k1 *= C2;
                        h1 ^= k1;
                        break;
                }
            }

            // finalization
            h1 ^= (ulong)len;
            h2 ^= (ulong)len;

            h1 += h2;
            h2 += h1;

            h1 = FMix64(h1);
            h2 = FMix64(h2);

            h1 += h2;
            h2 += h1;

            return h2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong FMix64(ulong k)
        {
            k ^= k >> 33;
            k *= 0xff51afd7ed558ccd;
            k ^= k >> 33;
            k *= 0xc4ceb9fe1a85ec53;
            k ^= k >> 33;
            return k;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint FMix32(uint h)
        {
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;
            return h;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Reverse(ulong value) =>
            ((value & 0x00000000000000FFUL) << 56) | ((value & 0x000000000000FF00UL) << 40) |
            ((value & 0x0000000000FF0000UL) << 24) | ((value & 0x00000000FF000000UL) << 08) |
            ((value & 0x000000FF00000000UL) >> 08) | ((value & 0x0000FF0000000000UL) >> 24) |
            ((value & 0x00FF000000000000UL) >> 40) | ((value & 0xFF00000000000000UL) >> 56);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Reverse(uint value) =>
            ((value & 0x000000FFU) << 24) | ((value & 0x0000FF00U) << 08) |
            ((value & 0x00FF0000U) >> 08) | ((value & 0xFF000000U) >> 24);
    }
}
