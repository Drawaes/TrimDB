using System;

namespace TrimDB.Core
{
    public readonly struct ValueLease : IDisposable
    {
        public static readonly ValueLease Empty = default;
        public static readonly ValueLease Deleted = new(default, null, true);

        public ReadOnlyMemory<byte> Value { get; }
        private readonly IDisposable? _owner;
        public bool IsDeleted { get; }
        public bool IsFound => !Value.IsEmpty;

        public ValueLease(ReadOnlyMemory<byte> value, IDisposable? owner, bool isDeleted = false)
        {
            Value = value;
            _owner = owner;
            IsDeleted = isDeleted;
        }

        public void Dispose() => _owner?.Dispose();
    }
}
