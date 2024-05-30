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
		Size: 0,
	}
}

func (header *SSTableHeader) SetSize(sz uint32) {
	header.Size = sz
}

func (header *SSTableHeader) IncSize(sz uint32) {
	header.Size += sz
}

type SSTable struct {
	header   SSTableHeader
	Index    map[string]uint32 // key: byte offset
	Data     []Block
	FileName string
}

func NewSSTable() *SSTable {
	return &SSTable{
		header: *NewSSTableHeader(),
		Index:  make(map[string]uint32),
		Data:   make([]Block, 0),
	}
}

func (s *SSTable) AddIndex(key string, offset uint32) {
	s.Index[key] = offset
}

func (s *SSTable) CreateBlock(entries []KeyValue) error {
	/*
		iterate over each KeyValue pair
		add it to block
		update index
		add block to s.Data
		update header
	*/
	var offset uint32
	var err error

	b := *NewBlock()
	for i := 0; i < len(entries); i++ {
		// add k-v to block
		err = b.Put(entries[i])
		if err != nil {
			return err
		}
		// update index
		offset = b.header.GetOffset()
		s.AddIndex(entries[i].Key, offset)
	}

	s.Data = append(s.Data, b)
	// update header
	s.header.IncSize(BLOCK_SIZE)
	return nil
}

func (s *SSTable) Write() error {
	/*
		storing the contents of SSTable to disk.

		file layout
		 0            4        m         n
		---------------------------------
		| indexOffset |  data  |  index  |
		---------------------------------
		indexOffset: byte offset from where index begins
		data: s.Data (key-value data)
		index: s.Index
	*/

	// open file desc (os.O_RDWR, 0666)
	file, err := os.Create(s.FileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// write indexOffset
	var indexOffset uint32 = uint32(len(s.Data) + 4)
	err = binary.Write(file, binary.LittleEndian, indexOffset)
	if err != nil {
		return err
	}

	// write s.Data
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(s.Data)
	if err != nil {
		return err
	}
	dataBytes := buf.Bytes()
	file.Write(dataBytes)

	// create buffer
	buf = new(bytes.Buffer)
	// create serializer
	enc = gob.NewEncoder(buf)
	// serialize s.Index
	err = enc.Encode(s.Index)
	if err != nil {
		return err
	}
	// convert to byte array
	indexBytes := buf.Bytes()
	// write to file
	file.Write(indexBytes)

	return nil
}

func (s *SSTable) Read() error {

	file, err := os.Open(s.FileName)
	if err != nil {
		return err
	}

	var indexOffset uint32
	// read first 4 bytes and store in indexOffset
	binary.Read(file, binary.LittleEndian, &indexOffset)

	// goal: read k-v data
	// 1. Get size of s.Data
	dataSize := int64(indexOffset) - 4
	// 2. create empty byte array
	dataBytes := make([]byte, dataSize)
	// 3. move pointer to start of k-v data
	file.Seek(4, 0)
	// 4. Read data into byte array
	file.Read(dataBytes)

	// goal: deserialize k-v data into s.Data
	// 1. create buffer with data byte array
	buf := bytes.NewBuffer(dataBytes)
	// 2. create decoder
	dec := gob.NewDecoder(buf)
	// 3. deserialize k-v data into s.Data
	err = dec.Decode(&s.Data)
	if err != nil {
		return err
	}

	// goal: read index from file
	// 1. get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	// 2. get size of index
	indexSize := fileSize - (dataSize + 4)
	// 3. create byte array
	indexBytes := make([]byte, indexSize)
	// 4. move pointer to start of index
	file.Seek(int64(indexOffset), 0)
	// 5. read index into byte array
	file.Read(indexBytes)

	// goal: deserialize index into s.Index
	// 1. create buffer with index byte array
	buf = bytes.NewBuffer(indexBytes)
	// 2. create decoder
	dec = gob.NewDecoder(buf)
	// 3. deserializer index data into s.Index
	err = dec.Decode(&s.Index)
	if err != nil {
		return err
	}

	return nil
}
