package xitdb

// ReadCountedHashMap

type ReadCountedHashMap struct {
	*ReadHashMap
}

func NewReadCountedHashMap(cursor *ReadCursor) (*ReadCountedHashMap, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagCountedHashMap, TagCountedHashSet:
		return &ReadCountedHashMap{ReadHashMap: &ReadHashMap{Cursor: cursor}}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (m *ReadCountedHashMap) Count() (int64, error) {
	return m.Cursor.Count()
}

// WriteCountedHashMap

type WriteCountedHashMap struct {
	*WriteHashMap
}

func NewWriteCountedHashMap(cursor *WriteCursor) (*WriteCountedHashMap, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagCountedHashMap, TagCountedHashSet:
	default:
		return nil, ErrUnexpectedTag
	}

	wc, err := cursor.WritePath([]PathPart{HashMapInitPart{Counted: true, Set: false}})
	if err != nil {
		return nil, err
	}
	rm := &ReadHashMap{Cursor: wc.ReadCursor}
	return &WriteCountedHashMap{WriteHashMap: &WriteHashMap{ReadHashMap: rm, writeCursor: wc}}, nil
}

func (m *WriteCountedHashMap) Count() (int64, error) {
	return m.Cursor.Count()
}
