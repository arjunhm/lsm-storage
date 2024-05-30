# Log Structured Storage

## MemTable
- in-memory. append only.
- all operations go here.
- keep it simple for now. like array of K-V pairs. implement skiplist later.
- flush to disk once size exceeds limit.

### Attributes
- size: the current size of the memtable
- limit: the max size it can accomodate.
- entries: list of key-value pairs

### Operations
- get(kv): returns kv pair if found, else error.
- put(kv): appends key-value pair. updates size. if `size` > `limit`, flush.
- delete(kv): appends key-value pair but marks as deleted. calls `put(kv)`.
- clear(): clears the contents of memtable.
- flush(): flushes contents of memtable to disk, and calls `clear()`.

# SSTable
- append-only immutable page.
- unsorted for now.
- merge with other SSTable once condition is met (determine condition later)
- has its own index?? maps key to byte offset of value
- Data: `<key-size, val-size, key-data, val-data>`

### Attributes
- header:
- index: maps key to byte offset
- data: byte array of key-value pairs
- file: file on disk

### Operations
- create(data): creates file on disk with data. builds index.
- write(): writes contents to disk
- read(): loads data from disk
