package xitdb

import "iter"

// ReadHashSet

type ReadHashSet struct {
	Cursor *ReadCursor
}

func NewReadHashSet(cursor *ReadCursor) (*ReadHashSet, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagHashMap, TagHashSet:
		return &ReadHashSet{Cursor: cursor}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (s *ReadHashSet) Slot() Slot {
	return s.Cursor.Slot()
}

func (s *ReadHashSet) All() iter.Seq2[*ReadCursor, error] {
	return s.Cursor.All()
}

func (s *ReadHashSet) GetCursor(key string) (*ReadCursor, error) {
	hash := s.Cursor.DB.digest([]byte(key))
	return s.GetCursorByHash(hash)
}

func (s *ReadHashSet) GetSlot(key string) (Slot, error) {
	hash := s.Cursor.DB.digest([]byte(key))
	return s.GetSlotByHash(hash)
}

func (s *ReadHashSet) GetCursorByBytes(key Bytes) (*ReadCursor, error) {
	hash := s.Cursor.DB.digest(key.Value)
	return s.GetCursorByHash(hash)
}

func (s *ReadHashSet) GetSlotByBytes(key Bytes) (Slot, error) {
	hash := s.Cursor.DB.digest(key.Value)
	return s.GetSlotByHash(hash)
}

func (s *ReadHashSet) GetCursorByHash(hash []byte) (*ReadCursor, error) {
	return s.Cursor.ReadPath([]PathPart{HashMapGetPart{Target: HashMapGetKey{Hash: hash}}})
}

func (s *ReadHashSet) GetSlotByHash(hash []byte) (Slot, error) {
	return s.Cursor.ReadPathSlot([]PathPart{HashMapGetPart{Target: HashMapGetKey{Hash: hash}}})
}

// WriteHashSet

type WriteHashSet struct {
	*ReadHashSet
	writeCursor *WriteCursor
}

func NewWriteHashSet(cursor *WriteCursor) (*WriteHashSet, error) {
	wc, err := cursor.WritePath([]PathPart{HashMapInitPart{Counted: false, Set: true}})
	if err != nil {
		return nil, err
	}
	rs, err := NewReadHashSet(wc.ReadCursor)
	if err != nil {
		return nil, err
	}
	return &WriteHashSet{ReadHashSet: rs, writeCursor: wc}, nil
}

func (s *WriteHashSet) Put(key string) error {
	b := []byte(key)
	hash := s.Cursor.DB.digest(b)
	return s.PutByHash(hash, NewBytes(b))
}

func (s *WriteHashSet) PutCursor(key string) (*WriteCursor, error) {
	hash := s.Cursor.DB.digest([]byte(key))
	return s.PutCursorByHash(hash)
}

func (s *WriteHashSet) Remove(key string) (bool, error) {
	hash := s.Cursor.DB.digest([]byte(key))
	return s.RemoveByHash(hash)
}

func (s *WriteHashSet) PutBytes(key Bytes) error {
	hash := s.Cursor.DB.digest(key.Value)
	return s.PutByHash(hash, key)
}

func (s *WriteHashSet) PutCursorByBytes(key Bytes) (*WriteCursor, error) {
	hash := s.Cursor.DB.digest(key.Value)
	return s.PutCursorByHash(hash)
}

func (s *WriteHashSet) RemoveByBytes(key Bytes) (bool, error) {
	hash := s.Cursor.DB.digest(key.Value)
	return s.RemoveByHash(hash)
}

func (s *WriteHashSet) PutByHash(hash []byte, data WriteableData) error {
	cursor, err := s.writeCursor.WritePath([]PathPart{
		HashMapGetPart{Target: HashMapGetKey{Hash: hash}},
	})
	if err != nil {
		return err
	}
	return cursor.WriteIfEmpty(data)
}

func (s *WriteHashSet) PutCursorByHash(hash []byte) (*WriteCursor, error) {
	return s.writeCursor.WritePath([]PathPart{
		HashMapGetPart{Target: HashMapGetKey{Hash: hash}},
	})
}

func (s *WriteHashSet) RemoveByHash(hash []byte) (bool, error) {
	_, err := s.writeCursor.WritePath([]PathPart{HashMapRemovePart{Hash: hash}})
	if err != nil {
		if err == ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *WriteHashSet) All() iter.Seq2[*WriteCursor, error] {
	return s.writeCursor.All()
}
