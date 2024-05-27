package lsm

const MEM_TABLE_LIMIT uint32 = 4096 * 1024 // 4MB

type MemTable struct {
	Entries []KeyValue
	Size    uint32
	Limit   uint32
}

func NewMembTale() *MemTable {
	return &MemTable{
		Entries: make([]KeyValue, 0),
		Size:    0,
		Limit:   MEM_TABLE_LIMIT,
	}
}

func (mt *MemTable) Get(key string) (string, error) {
	for i := len(mt.Entries) - 1; i > 0; i-- {
		if mt.Entries[i].Key == key {
			return mt.Entries[i].Value, nil
		}
	}
	return "", errors.New("key not found")
}

func (mt *MemTable) Put(key, val string, del bool) error {
	kv := NewKeyValue(key, val, del)
	mt.Entries = append(mt.Entries, kv)
	err := mt.IncSize(kv.Size)
	if err != nil {
		return err
	}

	if mt.IsFull() {
		err = mt.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (mt *MemTable) Delete(key string) error {
	return Add(key, "", true)
}

func (mt *MemTable) flush() error {
	// write it to disk

	// reset memtable
	err := mt.clear()
	if err != nil {
		return err
	}
	return nil

}

func (mt *MemTable) clear() error {
	mt.Entries = []Entry{}
	mt.Size = 0
	return nil
}

func (mt *MemTable) GetSize() uint32 {
	return mt.Size
}

func (mt *MemTable) IncSize(sz uint32) error {
	mt.Size += sz
	return nil
}

func (mt *MemTable) IsFull() bool {
	return mt.Size >= mt.Limit
}
