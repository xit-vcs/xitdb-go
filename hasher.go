package xitdb

import (
	"encoding/binary"
	"hash"
)

type Hasher struct {
	Hash hash.Hash
	ID   uint32
}

func BytesToID(name [4]byte) uint32 {
	return binary.BigEndian.Uint32(name[:])
}

func IDToBytes(id uint32) [4]byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], id)
	return buf
}
