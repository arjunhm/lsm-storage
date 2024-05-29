## LSM Storage

## Components

### MemTable
- in-memory. append only.
- all operations go here.
- keep it simple for now. like array of K-V pairs. implement skiplist later.
- flush to disk once size exceeds limit.

#### Attributes
- size: the current size of the memtable
- limit: the max size it can accomodate.
- entries: list of key-value pairs

#### Operations
- get(kv): returns kv pair if found, else error.
- put(kv): appends key-value pair. updates size. if `size` > `limit`, flush.
- delete(kv): appends key-value pair but marks as deleted. calls `put(kv)`.
- clear(): clears the contents of memtable.
- flush(): flushes contents of memtable to disk, and calls `clear()`.
