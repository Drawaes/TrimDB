using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace TrimDB.Core.SkipList
{
    public ref struct SkipListNode
    {
        private readonly Span<byte> _nodeMemory;
        private readonly long _location;

        public SkipListNode(Span<byte> memory, long location)
        {
            _nodeMemory = memory;
            _location = location;
        }

        public SkipListNode(Span<byte> memory, long location, byte height, ReadOnlySpan<byte> key)
            : this(memory, location)
        {
            ref var pointer = ref MemoryMarshal.GetReference(memory);
            ref var tableHeight = ref Unsafe.Add(ref pointer, 4);
            tableHeight = height;

            key.CopyTo(memory.Slice(16 + (height << 3)));

            ref var length = ref Unsafe.As<byte, int>(ref pointer);
            length = memory.Length;

        }

        // Node layout
        // ¦-----------------¦------------¦----------------¦
        // ¦ Field           ¦ Bit Size   ¦  byte no       ¦
        // ¦-----------------------------------------------¦
        // ¦ Node Length     ¦  32        ¦     0          ¦
        // ¦ Table Height    ¦   8        ¦     4          ¦
        // ¦ Reserved        ¦  24        ¦     5          ¦
        // ¦ Value Pointer   ¦  64        ¦     8          ¦
        // ¦ List Pointer    ¦  64        ¦    16          ¦
        // ¦ List Pointer x  ¦  64        ¦    16 + x  * 8 ¦ 
        // ¦ Key Data        ¦  Remainder ¦                ¦
        // ¦-----------------------------------------------¦

        public static int CalculateSizeNeeded(byte height, int keyLength)
        {
            var length = 16 + (height << 3) + keyLength;
            return length;
        }

        public long ValueLocation => Unsafe.Add(ref Unsafe.As<byte, long>(ref MemoryMarshal.GetReference(_nodeMemory)), 1);
        public byte TableHeight => Unsafe.Add(ref MemoryMarshal.GetReference(_nodeMemory), 4);
        public long Location => _location;
        public bool IsAllocated => _nodeMemory.Length != 0;
        public ReadOnlySpan<byte> Key => _nodeMemory.Slice(16 + (TableHeight << 3));

        public void SetValueLocation(long newValue)
        {
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, long>(ref MemoryMarshal.GetReference(_nodeMemory)), 1);
            Interlocked.Exchange(ref pointer, newValue);
        }

        public bool SetValueLocation(long previousValue, long newValue)
        {
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, long>(ref MemoryMarshal.GetReference(_nodeMemory)), 1);
            return Interlocked.CompareExchange(ref pointer, newValue, previousValue) == previousValue;
        }

        public long GetTableLocation(byte height)
        {
            Debug.Assert(height < TableHeight);

            var tableOffset = 2 + height;
            return Unsafe.Add(ref Unsafe.As<byte, long>(ref MemoryMarshal.GetReference(_nodeMemory)), tableOffset);
        }

        public void SetTablePointer(byte height, long nextLocation)
        {
            Debug.Assert(height < TableHeight);

            var tableOffset = 2 + height;
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, long>(ref MemoryMarshal.GetReference(_nodeMemory)), tableOffset);
            Interlocked.Exchange(ref pointer, nextLocation);
        }

        public bool SetTablePointer(byte height, long previousLocation, long nextLocation)
        {
            Debug.Assert(height < TableHeight);

            var tableOffset = 2 + height;
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, long>(ref MemoryMarshal.GetReference(_nodeMemory)), tableOffset);
            return Interlocked.CompareExchange(ref pointer, nextLocation, previousLocation) == previousLocation;
        }
    }
}
