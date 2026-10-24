package xitdb

import (
	"encoding/binary"
)

// stores compaction offsets in a hash map in a separate scratch database
// (typically a temporary file), without retaining the mappings in memory.
// create it with the cursor that Database.RunContext gives you on the
// scratch database, and run the compaction inside that same transaction,
// so the offsets aren't each committed separately. the scratch database's
// hash size must be at least 8 bytes.
type FileOffsetMap struct {
	m *WriteHashMap
}

func NewFileOffsetMap(cursor *WriteCursor) (*FileOffsetMap, error) {
	m, err := NewWriteHashMap(cursor)
	if err != nil {
		return nil, err
	}
	return &FileOffsetMap{m: m}, nil
}

func (m *FileOffsetMap) Reset() error {
	cursor := m.m.writeCursor
	if err := cursor.Write(nil); err != nil {
		return err
	}
	var err error
	m.m, err = NewWriteHashMap(cursor)
	return err
}

func (m *FileOffsetMap) Get(sourceOffset int64) (int64, bool, error) {
	cursor, err := m.m.GetCursorByHash(m.key(sourceOffset))
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
	return m.m.PutByHash(m.key(sourceOffset), NewUint(uint64(targetOffset)))
}

func (m *FileOffsetMap) key(offset int64) []byte {
	// encode the offset directly rather than hashing it, preserving exact keys
	key := make([]byte, m.m.Cursor.DB.Header.HashSize)
	binary.BigEndian.PutUint64(key[len(key)-8:], uint64(offset))
	return key
}
