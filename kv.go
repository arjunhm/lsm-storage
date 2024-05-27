package lsm

type KeyValue struct {
	Key string
	Value string
	Deleted bool
	Size uint32
}

func NewKeyValue(key, val string, deleted bool) *KeyValue {
	kv := KeyValue{
		Key: key,
		Value: val,
		Deleted: false,
		Size: 0,
	}
	kv.Size = uint32(len(kv.Key) + uint32(len(kv.Value))
	return &kv
}

func (kv *KeyValue) GetKeySize() uint32 {
	return uint32(len(kv.Key))
}

func (kv *KeyValue) GetValueSize() uint32 {
	return uint32(len(kv.Value))
}

