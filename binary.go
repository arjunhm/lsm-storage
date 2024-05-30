package lsm

import (
	"encoding/binary"
)

func Putuint32(buf []byte, val uint32) {
	binary.LittleEndian.PutUint32(buf, val)
}

func Getuint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}
