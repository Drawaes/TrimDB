﻿using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.IO.Pipelines;


namespace TrimDB.Core.KVLog
{

    public struct PutOperation
    {
        public Memory<byte> Key;
        public Memory<byte> Value;
        public bool Deleted;
        public TaskCompletionSource<long> LoggingCompleted;
    }

    public abstract class KVLogManager
    {
        public abstract Task<bool> IsAllCommitted();
        public abstract Task<long> LogKV(Memory<byte> key, Memory<byte> value, bool isDeleted);
        public abstract Task RecordCommitted(long offset);
        public abstract Task<Memory<byte>> ReadValueAtLocation(long offset);
        public abstract IAsyncEnumerable<PutOperation> GetUncommittedOperations();
    }

    public class FileBasedKVLogManager : KVLogManager
    {
        // Indicates if writes should be awaited before returning 
        protected bool _waitForFlush;

        // Not used yet - just placeholder for different strategies
        protected bool _waitForBuffer;

        // Not used yet - just placeholder for different strategies
        protected int _bufferSize;

        // channel for storage to write put operations for logging
        protected Channel<PutOperation> _channel;

        // long running task that pulls put operations, stores them and calls back with a value offset
        Task _readOperations;

        // current offset in the log
        private long _offset;
        private readonly PipeWriter _kvLogWriter;
        private readonly FileStream _kvLogStream;

        // Log filename - stores ksize,key,vsize,val (this needs to be improved with blocks and mmap)
        string _fileName;

        // Metadata filename - where highest 
        string _metadataFileName;

        public FileBasedKVLogManager(string fileName, string metadataFileName, bool waitForFlush = true, bool waitForFullBuffer = false, int bufferSize = 0)
        {
            // open metadata file & read last committed offset
            _metadataFileName = metadataFileName;

            // open or create value log
            _fileName = fileName;
            var f = new FileInfo(_fileName);
            _offset = 0;
            if (f.Exists)
            {
                _offset = f.Length;
            }

            // check all is ok and apply updates if not.
            // CheckState();

            _kvLogStream = new FileStream(_fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            _kvLogWriter = PipeWriter.Create(_kvLogStream);

            // create channel for Storage to write to
            _channel = Channel.CreateUnbounded<PutOperation>();

            // create and start task to consume incoming put operations
            _readOperations = new Task(() =>
            {
                ConsumeOperationsFromChannel();
            });
            _readOperations.Start();
        }

        // call this on startup to see if we need to apply lost updates
        public override async Task<bool> IsAllCommitted()
        {
            var lastCommitted = await ReadOffset();
            return _offset == lastCommitted;
        }

        public override async Task<long> LogKV(Memory<byte> key, Memory<byte> value, bool isDeleted)
        {
            var c = GetChannelWriter();
            var tcs = new TaskCompletionSource<long>();
            var po = new PutOperation();
            po.LoggingCompleted = tcs;
            po.Key = key;
            po.Value = value;
            po.Deleted = isDeleted;
            await c.WriteAsync(po);
            var off = await po.LoggingCompleted.Task;
            return off;
        }

        // Store the put operation in the kvLog. Either return immediately or await for operation to be written to disk.
        protected async Task ConsumeOperationsFromChannel()
        {
            // block async reading from channel
            while (true)
            {
                var op = await _channel.Reader.ReadAsync();
                WritePutOperation(op);

                // flush log
                if (_waitForFlush) {
                    await _kvLogWriter.FlushAsync();
                }
                else
                {
                    _ = _kvLogWriter.FlushAsync();
                }

                // call back to storage
                var valueOffset = _offset + sizeof(int) + op.Key.Length;

                // op.Completed(valueOffset);
                op.LoggingCompleted.SetResult(valueOffset);

                // update offset to end of value
                _offset = valueOffset + sizeof(int) + op.Value.Length;
            }
        }

        private void WritePutOperation(PutOperation op)
        {
            var kvSize = (int)(sizeof(int) + sizeof(int) + op.Key.Length + op.Value.Length);
            var span = _kvLogWriter.GetSpan(kvSize);
            BinaryPrimitives.WriteInt32LittleEndian(span, op.Key.Length);
            span = span[sizeof(int)..];
            op.Key.Span.CopyTo(span);
            span = span[op.Key.Length..];
            BinaryPrimitives.WriteInt32LittleEndian(span, op.Value.Length);
            span = span[sizeof(int)..];
            op.Value.Span.CopyTo(span);
            _kvLogWriter.Advance(kvSize);
        }

        public override async Task RecordCommitted(long offset)
        {
            // this is actually the offset off the start of the value.
            // so needs to read that location, get the length and then use that
            // to calc the start pos of the next key.
            using var fs = System.IO.File.OpenWrite(_metadataFileName);
            var pw = PipeWriter.Create(fs);
            var lastVal = await ReadValueAtLocation(offset);
            WriteOffset(pw, offset + sizeof(int) + lastVal.Length);
            await pw.FlushAsync();
        }

        private void WriteOffset(PipeWriter pipeWriter, long offset)
        {
            var span = pipeWriter.GetSpan(sizeof(long));
            BinaryPrimitives.WriteInt64LittleEndian(span, offset);
            pipeWriter.Advance(sizeof(long));
        }

        public override async Task<Memory<byte>> ReadValueAtLocation(long offset)
        {
            using var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            fs.Seek(offset, SeekOrigin.Begin);
            var sizeBuffer = new byte[sizeof(int)];
            await fs.ReadAsync(sizeBuffer, 0, sizeof(int));
            var valSize = BitConverter.ToInt32(sizeBuffer);
            var valBuffer = new byte[valSize];
            await fs.ReadAsync(valBuffer, 0, valSize);
            return new Memory<byte>(valBuffer, 0, valSize);
        }


        public async Task<long> ReadOffset()
        {
            using var fs = System.IO.File.OpenRead(_metadataFileName);
            var offsetBuffer = new byte[sizeof(long)];
            await fs.ReadAsync(offsetBuffer, 0, sizeof(long));
            var offset = BitConverter.ToInt64(offsetBuffer);
            return offset;
        }

        public override async IAsyncEnumerable<PutOperation> GetUncommittedOperations()
        {
            using var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            var committedOffset = await ReadOffset();
            fs.Seek(committedOffset, SeekOrigin.Begin);

            var sizeBuffer = new byte[sizeof(int)];
            var keySize = 0;
            var valSize = 0;
            while (committedOffset < _offset)
            {
                // create operations
                var putOperation = new PutOperation();

                // read key
                await fs.ReadAsync(sizeBuffer, 0, sizeof(int));
                keySize = BitConverter.ToInt32(sizeBuffer);
                var keyBuffer = new byte[keySize];
                await fs.ReadAsync(keyBuffer, 0, keySize);

                // read value
                await fs.ReadAsync(sizeBuffer, 0, sizeof(int));
                valSize = BitConverter.ToInt32(sizeBuffer);
                var valBuffer = new byte[valSize];
                await fs.ReadAsync(valBuffer, 0, valSize);

                // forward offset
                committedOffset += sizeof(int) + sizeof(int) + keySize + valSize;

                putOperation.Key = keyBuffer;
                putOperation.Value = valBuffer;

                // yield
                yield return putOperation;
            }
        }

        private ChannelWriter<PutOperation> GetChannelWriter()
        {
            return _channel.Writer;
        }
    }
}
