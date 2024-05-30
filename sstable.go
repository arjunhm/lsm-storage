package lsm

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"os"
)

type SSTableHeader struct {
	Size uint32
}

func NewSSTableHeader() *SSTableHeader {
	return &SSTableHeader{
		Size: MEM_TABLE_LIMIT,
	}
}

type SSTable struct {
	header   SSTableHeader
	Index    map[string]uint32 // key: byte offset
	Data     []Block
	FileName string
}

func NewSSTable() *SSTable {
	return &SSTable{
		header: NewSSTableHeader(),
		Index:  make(map[string]uint32),
		Data:   make([]Block, 0),
	}
}

func (s *SSTable) Write() error {
	// create file
	file, err := os.OpenFile(s.FileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	// write key-value data to file
	file.Write(s.Data)
	// get offset from where index starts, write to EOF
	indexOffset := len(s.Data)

	// serialize index
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(s.Index)
	if err != nil {
		return err
	}
	// convert serialized index to byte array
	indexBytes := buf.Bytes()
	// write byte array
	file.Write(indexOffset)
	// write index offset to last 4 bytes
	err = binary.Write(file, binary.LittleEndian, uint32(indexOffset))
	if err != nil {
		return err
	}
	return nil
}

func (s *SSTable) Read() error {
	var indexOffset uint32
	file, err := os.Open(s.FileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// get file info
	fileInfo, err = file.Stat()
	if err != nil {
		return err
	}
	// get file size
	fileSize := fileInfo.Size()

	// goal: get indexOffset by reading last 4 bytes
	// 1. move pointer to n-4 bytes
	indexEnd := file.Seek(-4, os.SEEK_END)
	// 2. store value of file.bytes[n-4,n] in indexOffset
	binary.Read(file, binary.LittleEndian, &indexOffset)

	// goal: read index
	// 1. get size of index
	indexSize := fileSize - indexEnd
	// 2. create byte array
	indexBytes := make([]byte, indexSize)
	// 3. move pointer to start of index
	file.Seek(int64(indexOffset), os.SEEK_SET)
	// 4. read index into byte array
	file.Read(indexBytes)

	// goal: deserialize index
	// 1. create buffer with index byte array
	buf := bytes.NewBuffer(indexBytes)
	// 2. create decoder
	dec := gob.NewDecoder(buf)
	// 3. deserialize and store in s.Index
	err = dec.Decode(&s.Index)
	if err != nil {
		return err
	}

	// goal: read key-value data
	// 1. create empty byte array
	s.Data = make([]byte, indexOffset)
	// 2. move pointer to start of file
	file.Seek(0, os.SEEK_SET)
	// 3. store data in s.Data
	file.Read(s.Data)

	return nil
}
