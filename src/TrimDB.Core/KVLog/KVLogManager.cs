using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
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
        public bool IsCommitMarker;
        public bool IsBatchBegin;
        public bool IsBatchCommit;
        public long CommittedUpToOffset;
        public TaskCompletionSource<long> LoggingCompleted;
    }

    public abstract class KVLogManager : IAsyncDisposable
    {
        public Func<List<(PutOperation Op, long Offset)>, Task>? OnBatchFlushed { get; set; }

        public virtual long CurrentPosition => 0;

        public abstract Task<bool> IsAllCommitted();
        public abstract Task<long> LogKV(Memory<byte> key, Memory<byte> value, bool isDeleted);
        public abstract Task RecordCommitted(long offset);
        public abstract Task<Memory<byte>> ReadValueAtLocation(long offset);
        public abstract Task LogBatchBegin();
        public abstract Task LogBatchCommit();
        public abstract IAsyncEnumerable<PutOperation> GetUncommittedOperations();
        public abstract ValueTask DisposeAsync();
    }

    /// <summary>
    /// WAL entry wire format:
    /// [keyLength:4][key:N][deleted:1][valueLength:4][value:N]
    ///
    /// Commit marker wire format:
    /// [keyLength:4 = -2][committedUpToOffset:8]  (12 bytes total)
    ///
    /// The deleted byte is 1 for tombstones, 0 for normal entries.
    /// When deleted=1, valueLength is 0 and no value bytes follow.
    /// </summary>
    public class FileBasedKVLogManager : KVLogManager
    {
        private const int CommitMarkerSentinel = -2;
        private const int CommitMarkerSize = sizeof(int) + sizeof(long); // 12 bytes
        private const int BatchBeginSentinel = -3;  // 4 bytes, no payload
        private const int BatchCommitSentinel = -4; // 4 bytes, no payload
        private const int BatchSentinelSize = sizeof(int);

        private readonly bool _waitForFlush;
        private readonly Channel<PutOperation> _channel;
        private readonly Task _consumerTask;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        // current offset in the log (tracks actual file position, including commit markers)
        private long _offset;
        // position after the last data entry (excludes commit markers)
        private long _dataEndOffset;
        private readonly PipeWriter _kvLogWriter;
        private readonly FileStream _kvLogStream;

        /// <summary>
        /// The current end-of-log position. After LogKV returns, this reflects
        /// the byte position after the last written entry.
        /// </summary>
        public override long CurrentPosition => Volatile.Read(ref _offset);

        private readonly string _fileName;
        private readonly string? _metadataFileName;

        public FileBasedKVLogManager(string fileName, string? metadataFileName = null, bool waitForFlush = true, int channelCapacity = 4096)
        {
            _metadataFileName = metadataFileName;
            _fileName = fileName;
            _waitForFlush = waitForFlush;

            var f = new FileInfo(_fileName);
            _offset = f.Exists ? f.Length : 0;
            _dataEndOffset = 0; // updated by ReadOffset scan or live writes

            _kvLogStream = new FileStream(_fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            _kvLogStream.Seek(0, SeekOrigin.End);
            _kvLogWriter = PipeWriter.Create(_kvLogStream);

            _channel = Channel.CreateBounded<PutOperation>(new BoundedChannelOptions(channelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
            });
            _consumerTask = Task.Run(() => ConsumeOperationsFromChannel());
        }

        public override async Task<bool> IsAllCommitted()
        {
            var lastCommitted = await ReadOffset();
            return _dataEndOffset <= lastCommitted;
        }

        public override async Task<long> LogKV(Memory<byte> key, Memory<byte> value, bool isDeleted)
        {
            var tcs = new TaskCompletionSource<long>();
            var po = new PutOperation
            {
                LoggingCompleted = tcs,
                Key = key,
                Value = value,
                Deleted = isDeleted,
            };
            await _channel.Writer.WriteAsync(po);
            return await tcs.Task;
        }

        private async Task ConsumeOperationsFromChannel()
        {
            // Reusable batch buffer to avoid allocations per flush cycle
            var batch = new List<(PutOperation Op, long EntryStart)>();

            try
            {
                while (await _channel.Reader.WaitToReadAsync(_cts.Token))
                {
                    batch.Clear();

                    // Drain all immediately available operations into the batch.
                    // The first op is guaranteed by WaitToReadAsync; keep draining
                    // without awaiting so we batch everything queued up.
                    while (_channel.Reader.TryRead(out var op))
                    {
                        var entryStart = _offset;
                        try
                        {
                            if (op.IsBatchBegin)
                            {
                                WriteBatchBeginMarker();
                                _offset += BatchSentinelSize;
                            }
                            else if (op.IsBatchCommit)
                            {
                                WriteBatchCommitMarker();
                                _offset += BatchSentinelSize;
                            }
                            else if (op.IsCommitMarker)
                            {
                                WriteCommitMarker(op);
                                _offset += CommitMarkerSize;
                            }
                            else
                            {
                                WritePutOperation(op);
                                _offset += sizeof(int) + op.Key.Length + 1 + sizeof(int)
                                           + (op.Deleted ? 0 : op.Value.Length);
                                _dataEndOffset = _offset;
                            }
                            batch.Add((op, entryStart));
                        }
                        catch (Exception ex)
                        {
                            op.LoggingCompleted.TrySetException(ex);
                        }
                    }

                    if (batch.Count == 0) continue;

                    // Single flush for the entire batch — amortizes fsync cost
                    try
                    {
                        if (_waitForFlush)
                        {
                            await _kvLogWriter.FlushAsync();
                            await _kvLogStream.FlushAsync();
                        }
                        else
                        {
                            _ = _kvLogWriter.FlushAsync();
                        }

                        // Ground-truth position after flush
                        _offset = _kvLogStream.Position;

                        // Single-writer: callback runs on consumer thread,
                        // so memtable inserts happen sequentially in WAL order
                        if (OnBatchFlushed != null)
                            await OnBatchFlushed(batch);

                        // Ack all operations in the batch
                        foreach (var (batchOp, start) in batch)
                        {
                            batchOp.LoggingCompleted.SetResult(start);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Flush failed — fail all pending operations in the batch
                        foreach (var (batchOp, _) in batch)
                        {
                            batchOp.LoggingCompleted.TrySetException(ex);
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                // Clean shutdown
            }
        }

        /// <summary>
        /// Wire format: [keyLength:4][key:N][deleted:1][valueLength:4][value:N]
        /// </summary>
        private void WritePutOperation(PutOperation op)
        {
            var isDeleted = op.Deleted;
            var valueLength = isDeleted ? 0 : op.Value.Length;
            var kvSize = sizeof(int) + op.Key.Length + 1 + sizeof(int) + valueLength;

            var span = _kvLogWriter.GetSpan(kvSize);

            // Key length + key
            BinaryPrimitives.WriteInt32LittleEndian(span, op.Key.Length);
            span = span[sizeof(int)..];
            op.Key.Span.CopyTo(span);
            span = span[op.Key.Length..];

            // Deleted flag
            span[0] = isDeleted ? (byte)1 : (byte)0;
            span = span[1..];

            // Value length + value
            BinaryPrimitives.WriteInt32LittleEndian(span, valueLength);
            span = span[sizeof(int)..];
            if (!isDeleted && valueLength > 0)
            {
                op.Value.Span.CopyTo(span);
            }

            _kvLogWriter.Advance(kvSize);
        }

        private void WriteCommitMarker(PutOperation op)
        {
            var span = _kvLogWriter.GetSpan(CommitMarkerSize);
            BinaryPrimitives.WriteInt32LittleEndian(span, CommitMarkerSentinel);
            BinaryPrimitives.WriteInt64LittleEndian(span[sizeof(int)..], op.CommittedUpToOffset);
            _kvLogWriter.Advance(CommitMarkerSize);
        }

        public override async Task RecordCommitted(long offset)
        {
            var tcs = new TaskCompletionSource<long>();
            var op = new PutOperation
            {
                IsCommitMarker = true,
                CommittedUpToOffset = offset,
                LoggingCompleted = tcs,
            };
            await _channel.Writer.WriteAsync(op);
            await tcs.Task; // wait for it to be flushed to disk
        }

        public override async Task LogBatchBegin()
        {
            var tcs = new TaskCompletionSource<long>();
            var op = new PutOperation
            {
                IsBatchBegin = true,
                LoggingCompleted = tcs,
            };
            await _channel.Writer.WriteAsync(op);
            await tcs.Task;
        }

        public override async Task LogBatchCommit()
        {
            var tcs = new TaskCompletionSource<long>();
            var op = new PutOperation
            {
                IsBatchCommit = true,
                LoggingCompleted = tcs,
            };
            await _channel.Writer.WriteAsync(op);
            await tcs.Task;
        }

        private void WriteBatchBeginMarker()
        {
            var span = _kvLogWriter.GetSpan(BatchSentinelSize);
            BinaryPrimitives.WriteInt32LittleEndian(span, BatchBeginSentinel);
            _kvLogWriter.Advance(BatchSentinelSize);
        }

        private void WriteBatchCommitMarker()
        {
            var span = _kvLogWriter.GetSpan(BatchSentinelSize);
            BinaryPrimitives.WriteInt32LittleEndian(span, BatchCommitSentinel);
            _kvLogWriter.Advance(BatchSentinelSize);
        }

        public override async Task<Memory<byte>> ReadValueAtLocation(long offset)
        {
            // Ensure PipeWriter has flushed before reading from a separate stream
            await _kvLogWriter.FlushAsync();

            // offset points to entry start: [keyLen:4][key:N][deleted:1][valLen:4][val:N]
            using var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            fs.Seek(offset, SeekOrigin.Begin);

            var intBuffer = new byte[sizeof(int)];

            // Skip past key
            await fs.ReadExactlyAsync(intBuffer.AsMemory());
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(intBuffer);
            fs.Seek(keyLen, SeekOrigin.Current);

            // Skip deleted flag
            fs.Seek(1, SeekOrigin.Current);

            // Read value
            await fs.ReadExactlyAsync(intBuffer.AsMemory());
            var valSize = BinaryPrimitives.ReadInt32LittleEndian(intBuffer);
            var valBuffer = new byte[valSize];
            if (valSize > 0)
            {
                await fs.ReadExactlyAsync(valBuffer.AsMemory());
            }
            return new Memory<byte>(valBuffer, 0, valSize);
        }

        public async Task<long> ReadOffset()
        {
            // Ensure all buffered data is on disk before scanning
            await _kvLogWriter.FlushAsync();

            long lastCommitted = 0;
            long pos = 0;
            long dataEnd = 0;

            if (File.Exists(_fileName))
            {
                using var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                var intBuf = new byte[sizeof(int)];
                var longBuf = new byte[sizeof(long)];

                while (pos < fs.Length)
                {
                    fs.Position = pos;
                    if (await fs.ReadAsync(intBuf.AsMemory()) < sizeof(int)) break;
                    var keyLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);

                    if (keyLen == CommitMarkerSentinel)
                    {
                        if (await fs.ReadAsync(longBuf.AsMemory()) < sizeof(long)) break;
                        lastCommitted = BinaryPrimitives.ReadInt64LittleEndian(longBuf);
                        pos += CommitMarkerSize;
                    }
                    else if (keyLen == BatchBeginSentinel || keyLen == BatchCommitSentinel)
                    {
                        pos += BatchSentinelSize;
                    }
                    else if (keyLen > 0)
                    {
                        // Skip past: key(keyLen) + deleted(1) + valLen(4)
                        fs.Position = pos + sizeof(int) + keyLen + 1;
                        if (await fs.ReadAsync(intBuf.AsMemory()) < sizeof(int)) break;
                        var valLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);
                        var entrySize = sizeof(int) + keyLen + 1 + sizeof(int) + valLen;
                        pos += entrySize;
                        dataEnd = pos;
                    }
                    else
                    {
                        break; // unknown or corrupt
                    }
                }
            }

            // Update _dataEndOffset from scan (handles reopened WALs)
            if (dataEnd > Volatile.Read(ref _dataEndOffset))
                Volatile.Write(ref _dataEndOffset, dataEnd);

            // Migration fallback: if no in-band markers found, check legacy metadata file
            if (lastCommitted == 0 && _metadataFileName != null && File.Exists(_metadataFileName))
            {
                using var metaFs = File.OpenRead(_metadataFileName);
                if (metaFs.Length >= sizeof(long))
                {
                    var offsetBuffer = new byte[sizeof(long)];
                    await metaFs.ReadExactlyAsync(offsetBuffer.AsMemory());
                    lastCommitted = BinaryPrimitives.ReadInt64LittleEndian(offsetBuffer);
                }
            }

            return lastCommitted;
        }

        public override async IAsyncEnumerable<PutOperation> GetUncommittedOperations()
        {
            // Ensure all buffered data is on disk before reading
            await _kvLogWriter.FlushAsync();

            using var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            var committedOffset = await ReadOffset();
            fs.Seek(committedOffset, SeekOrigin.Begin);

            var intBuffer = new byte[sizeof(int)];
            var currentPos = committedOffset;
            List<PutOperation>? batchBuffer = null;

            while (currentPos < _offset)
            {
                // Read key length (or sentinel)
                if (await fs.ReadAsync(intBuffer.AsMemory()) < sizeof(int)) break;
                var keySize = BinaryPrimitives.ReadInt32LittleEndian(intBuffer);

                if (keySize == CommitMarkerSentinel)
                {
                    // Skip commit marker payload (8 bytes)
                    fs.Seek(sizeof(long), SeekOrigin.Current);
                    currentPos += CommitMarkerSize;
                    continue;
                }

                if (keySize == BatchBeginSentinel)
                {
                    currentPos += BatchSentinelSize;
                    batchBuffer = new List<PutOperation>();
                    continue;
                }

                if (keySize == BatchCommitSentinel)
                {
                    currentPos += BatchSentinelSize;
                    if (batchBuffer != null)
                    {
                        foreach (var op in batchBuffer)
                            yield return op;
                        batchBuffer = null;
                    }
                    continue;
                }

                if (keySize <= 0) break; // corrupt or unknown

                // Read key
                var keyBuffer = new byte[keySize];
                await fs.ReadExactlyAsync(keyBuffer.AsMemory());

                // Read deleted flag — single byte, no allocation
                var deletedRaw = fs.ReadByte();
                if (deletedRaw < 0) break;
                var isDeleted = deletedRaw != 0;

                // Read value length + value
                await fs.ReadExactlyAsync(intBuffer.AsMemory());
                var valSize = BinaryPrimitives.ReadInt32LittleEndian(intBuffer);
                var valBuffer = new byte[valSize];
                if (valSize > 0)
                {
                    await fs.ReadExactlyAsync(valBuffer.AsMemory());
                }

                // Advance position: keyLen(4) + key(N) + deleted(1) + valLen(4) + val(N)
                currentPos += sizeof(int) + keySize + 1 + sizeof(int) + valSize;

                var putOp = new PutOperation
                {
                    Key = keyBuffer,
                    Value = valBuffer,
                    Deleted = isDeleted,
                };

                if (batchBuffer != null)
                {
                    batchBuffer.Add(putOp);
                }
                else
                {
                    yield return putOp;
                }
            }
            // If batchBuffer is non-null here, the batch was incomplete — discard it
        }

        public override async ValueTask DisposeAsync()
        {
            // Signal the consumer to stop
            _channel.Writer.TryComplete();
            _cts.Cancel();

            // Wait for the consumer to finish
            try { await _consumerTask; } catch { }

            // Flush and dispose the PipeWriter and FileStream
            try { await _kvLogWriter.FlushAsync(); } catch { }
            try { await _kvLogWriter.CompleteAsync(); } catch { }
            await _kvLogStream.DisposeAsync();

            _cts.Dispose();
        }
    }
}
