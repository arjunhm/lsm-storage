package lsm

type KeyValue struct {
	Key     string
	Value   string
	Deleted bool
	Size    uint32
}

func NewKeyValue(key, val string, del bool) *KeyValue {
	kv := &KeyValue{
		Key:     key,
		Value:   val,
		Deleted: del,
	}
	kv.UpdateSize()
	return kv
}

func (kv *KeyValue) GetKeySize() uint32 {
	return uint32(len(kv.Key))
}

func (kv *KeyValue) GetValueSize() uint32 {
	return uint32(len(kv.Value))
}

func (kv *KeyValue) UpdateSize() {
	kv.Size = kv.GetKeySize() + kv.GetValueSize()
}

func (kv *KeyValue) GetSize() uint32 {
	return kv.Size
}
