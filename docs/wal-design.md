# Write Ahead Log Design

The WAL provides protection against lost updates if the database exists before the current modifications to the MemTable have not been persisted.

## Requirements

From a user perspective if I say put("key", "value") and TrimDB returns with OK, then my expectation is that this key value pair has been persisted and even in the event of a crash I can retrieve the value with get("key").

However, the MemTable is not persisted to disk when put() is invoked, it is just updating the MemTable. There for we need a mechanism that is recording the set of modification actions, such that they can be replayed upon restart after a crash. 

## Design

Introduce a component called the WalManager that has the responsibility for writing the WAL and checking on startup if any operations need to be applied. 

WalWriter: A class for writing the WAL log to disk.

WALReader: A class that reads the WAL from specific points and provides order operations to be performed.

WAL File Structure:

Variable length Records stored in fixed sized blocks. Blocks are padded with 0 if no data.

Taken from RocksDB (https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format) the following structure would suffice:

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       <--- kBlockSize ------>|<-- kBlockSize ------>|

  rn = variable size records
  P = Padding
```

Individual records are strutured like this in RocksDB (maybe we use the same):

```
+---------+-----------+-----------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Payload   |
+---------+-----------+-----------+--- ... ---+

CRC = 32bit hash computed over the payload using CRC
Size = Length of the payload data
Type = Type of record
       (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
       The type is used to group a bunch of records together to represent
       blocks that are larger than kBlockSize
Payload = Byte stream as long as specified by the payload size

```

Operation data structure is as follows:

```
+--------------+------+----------------+-------+
| keySize (2B) | key  | valueSize (2B) | value |
+--------------+------+----------------+-------+
```

### Storing Operations

The MemTable is provided with a WALManager on startup and each time a user calls put(k,v) the MemTable is updated and then WALManager is informed and has the responsibility of ensuring that operation is committed to disk. Once it returns OK then the put operation can also return OK to calling process.

What about transactions? I guess roughly the same but upon the transaction completing in the memtable.

When the MemTable is flushed to disk then the WALManager is notified and must store the offset of the last recorded WALRecord.  

### Recovery on Startup

On startup the WALManager loads its metadata. Which consists of the last known WAL record id that was known committed to disk. 

Open a WALReader from the point of the last known commit and provide a stream of operations that are applied to the MemTable. The MemTable is then flushed to disk and then normal service is resumed.

## Issues

- what to use for file access, filestream, mmap
- best way to minimise blocking
- concurrency issues - how not to be a bottleneck with many concurrent writers processes
- one file or many?
- cleanup - we dont need to keep everything. In theory after a restart that applies any lost updates we can throw away the WAL.
- how to identify the WALRecords? What do we actually store when MemTable is flushed.
- What if there is some corruption of the file. i.e. the CRC is not valid on read? 
- Are there options to NOT flush the WAL to disk on every PUT? Others have this option. It means that there is still some potential for lost updates.


