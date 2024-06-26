package lsm

import "errors"

const MEM_TABLE_LIMIT uint32 = 4096 * 1024 //4MB

type MemTable struct {
	Size    uint32
	Limit   uint32
	Entries []KeyValue
}

func NewMemTable() *MemTable {
	return &MemTable{
		Entries: make([]KeyValue, 0),
		Limit:   MEM_TABLE_LIMIT,
	}
}

// ----- HELPER -----
func (mt *MemTable) GetSize() uint32 {
	return mt.Size
}

func (mt *MemTable) incSize(sz uint32) {
	mt.Size += sz
	// flush here?
}

func (mt *MemTable) isFull() bool {
	return mt.Size >= mt.Limit
}

func (mt *MemTable) clear() error {
	mt.Entries = []KeyValue{}
	mt.Size = 0
	return nil
}

func (mt *MemTable) flush() error {
	/*
		create new SSTable
		iterate over KeyValue entries
			- increase slice until it exceeds block size
			- create block in SSTable
		write SSTable to disk
		clear MemTable
	*/
	var err error

	i := 0
	var curSize uint32 = 0
	sst := NewSSTable()

	for j := 0; j < len(mt.Entries); j++ {
		kvSize := mt.Entries[i].GetSize()
		if curSize+kvSize <= BLOCK_DATA_SIZE {
			curSize += kvSize
		} else {
			err = sst.CreateBlock(mt.Entries[i:j])
			if err != nil {
				return err
			}
			i = j
			curSize = 0
		}
	}

	// write SSTable to disk
	err = sst.Write()
	if err != nil {
		return err
	}

	// clear memtable
	err = mt.clear()
	if err != nil {
		return err
	}
	return nil
}

// ----- CRUD -----

// ? return KeyValue instead?
func (mt *MemTable) Get(key string) (string, error) {
	// naive for now 😭
	for i := len(mt.Entries) - 1; i >= 0; i-- {
		e := mt.Entries[i]
		if e.Key == key {
			// ! --- may have to change this later ---
			if e.Deleted == true {
				return "", nil
			} else {
				return e.Value, nil
			}
		}
	}

	return "", errors.New("key not found")
}

func (mt *MemTable) Put(kv KeyValue) error {
	var err error
	mt.Entries = append(mt.Entries, kv)
	mt.incSize(kv.Size)

	// update WAL
	if mt.isFull() {
		err = mt.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (mt *MemTable) Delete(key string) error {
	kv := *NewKeyValue(key, "", true)

	err := mt.Put(kv)
	if err != nil {
		return err
	}
	return nil
}
