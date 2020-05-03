TrimDB Data Structures
==

TrimDB adopts a LSM[1] approach to data storage and retrieval.

The TrimDB LSM approach comprises of several levels of data structure. Level c0 is special. It is an in-memory data structure that stores ordered key value pairs. Level c1 .. cn are Sorted String Tables (SSTables). SSTables are immutable on disk structures that contain keys and values, ordered by key.

Updates are only made to the in-memory c0 structure. Periodically, the c0 structure is written out to a new SSTable file. 

Each SSTable file also has an associated SSIndex file. The index file is also key ordered and contains an offset for each key into the SSTable file.  

TrimDB uses a skip-list for its in-memory c0 structure. 

When attempting to locate a key level c0 is checked first and then all SSTables in reverse timestamp order. Deletes are marked with tombstones.

Iterators are implemented by seeking to the relevant position in each file and the in-memory structure and iterating in parallel.

To improve key location a bloom filter is used to help know which files the key is not in.

A background task(s) are merging smaller SSTables into larger ones to reduce the number of files that need to be consulted to find a key. They complete this action then modify a common root structure that lists SSTables and contains the bloom filter.

Writes to the in-memory skip-list are not persisted immediately and thus any crash results in lost data. To protect against this all incoming updates are written to a write ahead log. After each time level c0 is stored as a new SSTable a write ahead watermark is stored. In the event of a messy shutdown / crash upon restart the database checks the watermark and replays any missed updates from the log before allowing new operations. 

Transactions. (unclear as of yet but.. ) Transactions offer isolation from other transactions. Thus, they cant read other transactions in progress, the txn writes are not exposed to others until committed. Also, if a key is modified by another transaction before this commits then the txn should fail and no changes committed. Simple approach could be to copy level c0, make all changes and read on that / plus SSTables (they are immutable so all good). Then on commit apply on changes to level c0 (doesnt sound great). Or maybe level c0 can mark each node with a txn number. We track the latest committed txn number. Give that and a new txn number to each txn. They update the skiplist in parallel, only read older values and if they both try and update the same key they can compare their txn numbers.    







Refs
--

1. https://www.cs.umb.edu/~poneil/lsmtree.pdf