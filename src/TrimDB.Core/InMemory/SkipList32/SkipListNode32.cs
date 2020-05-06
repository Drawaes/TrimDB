using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace TrimDB.Core.InMemory.SkipList32
{
    public ref struct SkipListNode32
    {
        private readonly Span<byte> _nodeMemory;
        private readonly int _location;

        public SkipListNode32(Span<byte> memory, int location)
        {
            _nodeMemory = memory;
            _location = location;
        }

        public SkipListNode32(Span<byte> memory, int location, byte height, ReadOnlySpan<byte> key)
            : this(memory, location)
        {
            ref var pointer = ref MemoryMarshal.GetReference(memory);
            ref var tableHeight = ref Unsafe.Add(ref pointer, 4);
            tableHeight = height;

            key.CopyTo(memory.Slice(12 + (height << 2)));

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
        // ¦ Value Pointer   ¦  32        ¦     8          ¦
        // ¦ List Pointer    ¦  32        ¦    12          ¦
        // ¦ List Pointer x  ¦  32        ¦    12 + x  * 4 ¦ 
        // ¦ Key Data        ¦  Remainder ¦                ¦
        // ¦-----------------------------------------------¦

        public static int CalculateSizeNeeded(byte height, int keyLength)
        {
            var length = 12 + (height << 2) + keyLength;
            return length;
        }

        public int ValueLocation => Unsafe.Add(ref Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(_nodeMemory)), 2);
        public byte TableHeight => Unsafe.Add(ref MemoryMarshal.GetReference(_nodeMemory), 4);
        public int Location => _location;
        public bool IsDeleted
        {
            get
            {
                ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(_nodeMemory)), 2);
                return (pointer & 0xF000_0000) > 0;
            }
        }
        public bool IsAllocated => _nodeMemory.Length != 0;
        public ReadOnlySpan<byte> Key => _nodeMemory.Slice(12 + (TableHeight << 2));

        public void SetDeleted()
        {
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(_nodeMemory)), 2);
            pointer |= unchecked((int)0xF000_0000);
        }

        public void SetValueLocation(int newValue)
        {
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(_nodeMemory)), 2);
            Interlocked.Exchange(ref pointer, newValue);
        }

        public bool SetValueLocation(int previousValue, int newValue)
        {
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, long>(ref MemoryMarshal.GetReference(_nodeMemory)), 2);
            return Interlocked.CompareExchange(ref pointer, newValue, previousValue) == previousValue;
        }

        public int GetTableLocation(byte height)
        {
            Debug.Assert(height < TableHeight);

            var tableOffset = 3 + height;
            return Unsafe.Add(ref Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(_nodeMemory)), tableOffset);
        }


        public void SetTablePointer(byte height, int nextLocation)
        {
            Debug.Assert(height < TableHeight);

            var tableOffset = 3 + height;
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(_nodeMemory)), tableOffset);
            Interlocked.Exchange(ref pointer, nextLocation);
        }

        public bool SetTablePointer(byte height, int previousLocation, int nextLocation)
        {
            Debug.Assert(height < TableHeight);

            var tableOffset = 3 + height;
            ref var pointer = ref Unsafe.Add(ref Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(_nodeMemory)), tableOffset);
            return Interlocked.CompareExchange(ref pointer, nextLocation, previousLocation) == previousLocation;
        }
    }
}
