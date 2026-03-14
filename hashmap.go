package xitdb

import "iter"

// ReadHashMap

type ReadHashMap struct {
	Cursor *ReadCursor
}

func NewReadHashMap(cursor *ReadCursor) (*ReadHashMap, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagHashMap, TagHashSet:
		return &ReadHashMap{Cursor: cursor}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (m *ReadHashMap) Slot() Slot {
	return m.Cursor.Slot()
}

func (m *ReadHashMap) All() iter.Seq2[*ReadCursor, error] {
	return m.Cursor.All()
}

// String key methods

func (m *ReadHashMap) GetCursor(key string) (*ReadCursor, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.GetCursorByHash(hash)
}

func (m *ReadHashMap) GetSlot(key string) (*Slot, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.GetSlotByHash(hash)
}

func (m *ReadHashMap) GetKeyCursor(key string) (*ReadCursor, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.GetKeyCursorByHash(hash)
}

func (m *ReadHashMap) GetKeySlot(key string) (*Slot, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.GetKeySlotByHash(hash)
}

func (m *ReadHashMap) GetKeyValuePair(key string) (*ReadKVPairCursor, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.GetKeyValuePairByHash(hash)
}

// Bytes key methods

func (m *ReadHashMap) GetCursorByBytes(key Bytes) (*ReadCursor, error) {
	hash := m.Cursor.DB.digest(key.Value)
	return m.GetCursorByHash(hash)
}

func (m *ReadHashMap) GetSlotByBytes(key Bytes) (*Slot, error) {
	hash := m.Cursor.DB.digest(key.Value)
	return m.GetSlotByHash(hash)
}

func (m *ReadHashMap) GetKeyCursorByBytes(key Bytes) (*ReadCursor, error) {
	hash := m.Cursor.DB.digest(key.Value)
	return m.GetKeyCursorByHash(hash)
}

func (m *ReadHashMap) GetKeySlotByBytes(key Bytes) (*Slot, error) {
	hash := m.Cursor.DB.digest(key.Value)
	return m.GetKeySlotByHash(hash)
}

func (m *ReadHashMap) GetKeyValuePairByBytes(key Bytes) (*ReadKVPairCursor, error) {
	hash := m.Cursor.DB.digest(key.Value)
	return m.GetKeyValuePairByHash(hash)
}

// Hash key methods

func (m *ReadHashMap) GetCursorByHash(hash []byte) (*ReadCursor, error) {
	return m.Cursor.ReadPath([]PathPart{HashMapGetPart{Target: HashMapGetValue{Hash: hash}}})
}

func (m *ReadHashMap) GetSlotByHash(hash []byte) (*Slot, error) {
	return m.Cursor.ReadPathSlot([]PathPart{HashMapGetPart{Target: HashMapGetValue{Hash: hash}}})
}

func (m *ReadHashMap) GetKeyCursorByHash(hash []byte) (*ReadCursor, error) {
	return m.Cursor.ReadPath([]PathPart{HashMapGetPart{Target: HashMapGetKey{Hash: hash}}})
}

func (m *ReadHashMap) GetKeySlotByHash(hash []byte) (*Slot, error) {
	return m.Cursor.ReadPathSlot([]PathPart{HashMapGetPart{Target: HashMapGetKey{Hash: hash}}})
}

func (m *ReadHashMap) GetKeyValuePairByHash(hash []byte) (*ReadKVPairCursor, error) {
	cursor, err := m.Cursor.ReadPath([]PathPart{HashMapGetPart{Target: HashMapGetKVPair{Hash: hash}}})
	if err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, nil
	}
	return cursor.ReadKeyValuePair()
}

// WriteHashMap

type WriteHashMap struct {
	*ReadHashMap
	writeCursor *WriteCursor
}

func NewWriteHashMap(cursor *WriteCursor) (*WriteHashMap, error) {
	wc, err := cursor.WritePath([]PathPart{HashMapInitPart{Counted: false, Set: false}})
	if err != nil {
		return nil, err
	}
	rm, err := NewReadHashMap(wc.ReadCursor)
	if err != nil {
		return nil, err
	}
	return &WriteHashMap{ReadHashMap: rm, writeCursor: wc}, nil
}

// String key methods

func (m *WriteHashMap) Put(key string, data WriteableData) error {
	hash := m.Cursor.DB.digest([]byte(key))
	if err := m.PutKeyByHash(hash, NewString(key)); err != nil {
		return err
	}
	return m.PutByHash(hash, data)
}

func (m *WriteHashMap) PutCursor(key string) (*WriteCursor, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	if err := m.PutKeyByHash(hash, NewString(key)); err != nil {
		return nil, err
	}
	return m.PutCursorByHash(hash)
}

func (m *WriteHashMap) PutKey(key string, data WriteableData) error {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.PutKeyByHash(hash, data)
}

func (m *WriteHashMap) PutKeyCursor(key string) (*WriteCursor, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.PutKeyCursorByHash(hash)
}

func (m *WriteHashMap) Remove(key string) (bool, error) {
	hash := m.Cursor.DB.digest([]byte(key))
	return m.RemoveByHash(hash)
}

// Bytes key methods

func (m *WriteHashMap) PutBytes(key Bytes, data WriteableData) error {
	hash := m.Cursor.DB.digest(key.Value)
	if err := m.PutKeyByHash(hash, key); err != nil {
		return err
	}
	return m.PutByHash(hash, data)
}

func (m *WriteHashMap) PutCursorByBytes(key Bytes) (*WriteCursor, error) {
	hash := m.Cursor.DB.digest(key.Value)
	if err := m.PutKeyByHash(hash, key); err != nil {
		return nil, err
	}
	return m.PutCursorByHash(hash)
}

func (m *WriteHashMap) PutKeyByBytes(key Bytes, data WriteableData) error {
	hash := m.Cursor.DB.digest(key.Value)
	return m.PutKeyByHash(hash, data)
}

func (m *WriteHashMap) PutKeyCursorByBytes(key Bytes) (*WriteCursor, error) {
	hash := m.Cursor.DB.digest(key.Value)
	return m.PutKeyCursorByHash(hash)
}

func (m *WriteHashMap) RemoveByBytes(key Bytes) (bool, error) {
	hash := m.Cursor.DB.digest(key.Value)
	return m.RemoveByHash(hash)
}

// Hash key methods

func (m *WriteHashMap) PutByHash(hash []byte, data WriteableData) error {
	_, err := m.writeCursor.WritePath([]PathPart{
		HashMapGetPart{Target: HashMapGetValue{Hash: hash}},
		WriteDataPart{Data: data},
	})
	return err
}

func (m *WriteHashMap) PutCursorByHash(hash []byte) (*WriteCursor, error) {
	return m.writeCursor.WritePath([]PathPart{
		HashMapGetPart{Target: HashMapGetValue{Hash: hash}},
	})
}

func (m *WriteHashMap) PutKeyByHash(hash []byte, data WriteableData) error {
	cursor, err := m.writeCursor.WritePath([]PathPart{
		HashMapGetPart{Target: HashMapGetKey{Hash: hash}},
	})
	if err != nil {
		return err
	}
	return cursor.WriteIfEmpty(data)
}

func (m *WriteHashMap) PutKeyCursorByHash(hash []byte) (*WriteCursor, error) {
	return m.writeCursor.WritePath([]PathPart{
		HashMapGetPart{Target: HashMapGetKey{Hash: hash}},
	})
}

func (m *WriteHashMap) RemoveByHash(hash []byte) (bool, error) {
	_, err := m.writeCursor.WritePath([]PathPart{HashMapRemovePart{Hash: hash}})
	if err != nil {
		if err == ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m *WriteHashMap) All() iter.Seq2[*WriteCursor, error] {
	return m.writeCursor.All()
}
