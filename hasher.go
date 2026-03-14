package xitdb

import (
	"encoding/binary"
	"fmt"
	"hash"
)

type Hasher struct {
	NewHash func() hash.Hash
	ID      uint32
}

func StringToID(name string) (uint32, error) {
	b := []byte(name)
	if len(b) != 4 {
		return 0, fmt.Errorf("name must be exactly four bytes long")
	}
	return binary.BigEndian.Uint32(b), nil
}

func IDToString(id uint32) string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], id)
	return string(buf[:])
}
