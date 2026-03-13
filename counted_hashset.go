package xitdb

// ReadCountedHashSet

type ReadCountedHashSet struct {
	*ReadHashSet
}

func NewReadCountedHashSet(cursor *ReadCursor) (*ReadCountedHashSet, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagCountedHashMap, TagCountedHashSet:
		return &ReadCountedHashSet{ReadHashSet: &ReadHashSet{Cursor: cursor}}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (s *ReadCountedHashSet) Count() (int64, error) {
	return s.Cursor.Count()
}

// WriteCountedHashSet

type WriteCountedHashSet struct {
	*WriteHashSet
}

func NewWriteCountedHashSet(cursor *WriteCursor) (*WriteCountedHashSet, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagCountedHashMap, TagCountedHashSet:
	default:
		return nil, ErrUnexpectedTag
	}

	wc, err := cursor.WritePath([]PathPart{HashMapInitPart{Counted: true, Set: true}})
	if err != nil {
		return nil, err
	}
	rs := &ReadHashSet{Cursor: wc.ReadCursor}
	return &WriteCountedHashSet{WriteHashSet: &WriteHashSet{ReadHashSet: rs, writeCursor: wc}}, nil
}

func (s *WriteCountedHashSet) Count() (int64, error) {
	return s.Cursor.Count()
}
