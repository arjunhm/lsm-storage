package lsm

import (
	"errors"
	"strings"
)

/*
BlockHeader layout

 0      4       8
----------------
| count | offset|
----------------

Block.Data layout
 0             4              8     k     v
------------------------------------------
| key-size (k) | val-size (v) | key | val |
------------------------------------------

*/

const (
	BLOCK_SIZE        uint32 = 4096 // 4KB
	BLOCK_HEADER_SIZE uint32 = 8    // 8B
	BLOCK_DATA_SIZE   uint32 = BLOCK_SIZE - BLOCK_HEADER_SIZE
	KEY_SIZE          uint32 = 4 // 4B
	VAL_SIZE          uint32 = 4 // 4B
)

// --- block header ---

type BlockHeader struct {
	count  uint32
	offset uint32
}

func NewBlockHeader() *BlockHeader {
	return &BlockHeader{
		count:  0,
		offset: BLOCK_HEADER_SIZE,
	}
}

func (bh *BlockHeader) SetOffset(offset uint32) {
	bh.offset = offset
}

func (bh *BlockHeader) GetOffset() uint32 {
	return bh.offset
}

// --- block ---

type Block struct {
	header BlockHeader
	Data   []byte
}

func NewBlock() *Block {
	return &Block{
		header: *NewBlockHeader(),
		Data:   make([]byte, BLOCK_DATA_SIZE),
	}
}

func (b *Block) Get(offset uint32, key string) (string, error) {

	if offset < 0 || offset >= BLOCK_DATA_SIZE {
		return "", errors.New("invalid offset")
	}

	var keyStart, keyEnd, valEnd uint32
	var keySize, valSize uint32
	var currKey, val string

	for {
		if offset < 0 || offset >= BLOCK_DATA_SIZE {
			break
		}
		keySize = Getuint32(b.Data[offset : offset+KEY_SIZE])
		keyStart = offset + KEY_SIZE + VAL_SIZE
		keyEnd = keyStart + keySize

		valSize = Getuint32(b.Data[offset+KEY_SIZE : keyStart])
		valEnd = keyEnd + valSize

		currKey = string(b.Data[keyStart:keyEnd])
		if key == currKey {
			val = string(b.Data[keyEnd:valEnd])
			return val, nil
		}
		// if currKey is lexicographically greater
		if strings.Compare(currKey, key) == 1 {
			break
		}
		offset = valEnd
	}

	return "", errors.New("value not found")
}

func (b *Block) Put(kv KeyValue) error {
	offset := b.header.GetOffset()
	if offset+kv.GetSize() > BLOCK_DATA_SIZE {
		return errors.New("insufficient space")
	}

	keySize := kv.GetKeySize()
	valSize := kv.GetValueSize()
	keyStart := offset + KEY_SIZE + VAL_SIZE
	keyEnd := keyStart + keySize
	valEnd := keyEnd + valSize

	Putuint32(b.Data[offset:offset+KEY_SIZE], keySize)
	Putuint32(b.Data[offset+KEY_SIZE:keyStart], valSize)
	copy(b.Data[keyStart:keyEnd], kv.Key)
	copy(b.Data[keyEnd:valEnd], kv.Value)

	b.header.SetOffset(keySize + KEY_SIZE + VAL_SIZE)
	b.header.count += 1

	return nil
}
