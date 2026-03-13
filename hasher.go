package xitdb

import (
	"encoding/binary"
	"fmt"
	"hash"
)

type Hasher struct {
	NewHash func() hash.Hash
	ID      int32
}

func StringToID(name string) (int32, error) {
	b := []byte(name)
	if len(b) != 4 {
		return 0, fmt.Errorf("name must be exactly four bytes long")
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func IDToString(id int32) string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(id))
	return string(buf[:])
}
