package xitdb

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
)

// stores compaction offsets in a separate scratch database without retaining
// the mappings in memory. the supplied file is truncated on construction and
// reset, and is closed on close or failed construction. the caller deletes it.
type FileOffsetMap struct {
	core *fileOffsetCore
	db   *Database
}

type fileOffsetCore struct {
	*CoreFile
}

func (c *fileOffsetCore) Sync() error {
	// scratch mappings do not need crash durability
	return nil
}

func NewFileOffsetMap(file *os.File) (*FileOffsetMap, error) {
	m := &FileOffsetMap{core: &fileOffsetCore{NewCoreFile(file)}}
	if err := m.Reset(); err != nil {
		return nil, errors.Join(err, m.Close())
	}
	return m, nil
}

func (m *FileOffsetMap) Reset() error {
	if err := m.core.SetLength(0); err != nil {
		return err
	}
	var err error
	m.db, err = NewDatabase(m.core, Hasher{Hash: sha256.New})
	return err
}

func (m *FileOffsetMap) Get(sourceOffset int64) (int64, bool, error) {
	index, err := NewReadHashMap(m.db.RootCursor().ReadCursor)
	if err != nil {
		return 0, false, err
	}
	key := fileOffsetKey(sourceOffset)
	cursor, err := index.GetCursorByHash(key[:])
	if err != nil {
		return 0, false, err
	}
	if cursor == nil {
		return 0, false, nil
	}
	targetOffset, err := cursor.ReadUint()
	if err != nil {
		return 0, false, err
	}
	return int64(targetOffset), true, nil
}

func (m *FileOffsetMap) Put(sourceOffset, targetOffset int64) error {
	index, err := NewWriteHashMap(m.db.RootCursor())
	if err != nil {
		return err
	}
	key := fileOffsetKey(sourceOffset)
	return index.PutByHash(key[:], NewUint(uint64(targetOffset)))
}

func fileOffsetKey(offset int64) [sha256.Size]byte {
	// encode the offset directly rather than hashing it, preserving exact keys
	var key [sha256.Size]byte
	binary.BigEndian.PutUint64(key[sha256.Size-8:], uint64(offset))
	return key
}

func (m *FileOffsetMap) Close() error {
	return m.core.Close()
}
