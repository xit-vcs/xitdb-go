package xitdb

import (
	"errors"
	"iter"
)

// ReadSortedMap

type ReadSortedMap struct {
	Cursor *ReadCursor
}

func NewReadSortedMap(cursor *ReadCursor) (*ReadSortedMap, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagSortedMap, TagSortedSet:
		return &ReadSortedMap{Cursor: cursor}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (m *ReadSortedMap) Slot() Slot {
	return m.Cursor.Slot()
}

func (m *ReadSortedMap) Count() (int64, error) {
	return m.Cursor.Count()
}

func (m *ReadSortedMap) All() iter.Seq2[*ReadCursor, error] {
	return m.Cursor.All()
}

// AllFrom iterates in key order starting at the first entry with key >= startKey
func (m *ReadSortedMap) AllFrom(startKey []byte) iter.Seq2[*ReadCursor, error] {
	cursor := m.Cursor
	return sortedIterSeq(func() (*CursorIterator, error) {
		return newSortedIterFromKey(cursor, startKey)
	})
}

// AllFromIndex iterates in key order starting at the entry with rank startIndex
func (m *ReadSortedMap) AllFromIndex(startIndex int64) iter.Seq2[*ReadCursor, error] {
	cursor := m.Cursor
	return sortedIterSeq(func() (*CursorIterator, error) {
		return newSortedIterFromIndex(cursor, startIndex)
	})
}

// String key methods

func (m *ReadSortedMap) GetCursor(key string) (*ReadCursor, error) {
	return m.GetCursorByBytes([]byte(key))
}

func (m *ReadSortedMap) GetSlot(key string) (Slot, error) {
	return m.GetSlotByBytes([]byte(key))
}

func (m *ReadSortedMap) GetKeyValuePair(key string) (*ReadKVPairCursor, error) {
	return m.GetKeyValuePairByBytes([]byte(key))
}

func (m *ReadSortedMap) Rank(key string) (int64, error) {
	return m.RankByBytes([]byte(key))
}

// Byte-slice key methods (sorted-map keys are byte strings, not hashes)

func (m *ReadSortedMap) GetCursorByBytes(key []byte) (*ReadCursor, error) {
	return m.Cursor.ReadPath([]PathPart{SortedMapGetPart{Target: SortedMapGetValue{Key: key}}})
}

func (m *ReadSortedMap) GetSlotByBytes(key []byte) (Slot, error) {
	return m.Cursor.ReadPathSlot([]PathPart{SortedMapGetPart{Target: SortedMapGetValue{Key: key}}})
}

func (m *ReadSortedMap) GetKeyValuePairByBytes(key []byte) (*ReadKVPairCursor, error) {
	cursor, err := m.Cursor.ReadPath([]PathPart{SortedMapGetPart{Target: SortedMapGetKVPair{Key: key}}})
	if err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, nil
	}
	return cursor.ReadKeyValuePair()
}

// GetIndexKeyValuePair returns the key/value pair at the given rank (negative counts from the end)
func (m *ReadSortedMap) GetIndexKeyValuePair(index int64) (*ReadKVPairCursor, error) {
	cursor, err := m.Cursor.ReadPath([]PathPart{SortedMapGetIndexPart{Index: index}})
	if err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, nil
	}
	return cursor.ReadKeyValuePair()
}

// RankByBytes returns the number of keys strictly less than key
func (m *ReadSortedMap) RankByBytes(key []byte) (int64, error) {
	if m.Cursor.SlotPtr.Slot.Tag == TagNone {
		return 0, nil
	}
	if err := m.Cursor.DB.Core.SeekTo(m.Cursor.SlotPtr.Slot.Value); err != nil {
		return 0, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := m.Cursor.DB.Core.Read(headerBytes[:]); err != nil {
		return 0, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return 0, err
	}
	return m.Cursor.DB.sortedRank(header.RootPtr, key)
}

// WriteSortedMap

type WriteSortedMap struct {
	*ReadSortedMap
	writeCursor *WriteCursor
}

func NewWriteSortedMap(cursor *WriteCursor) (*WriteSortedMap, error) {
	wc, err := cursor.WritePath([]PathPart{SortedMapInitPart{Set: false}})
	if err != nil {
		return nil, err
	}
	rm, err := NewReadSortedMap(wc.ReadCursor)
	if err != nil {
		return nil, err
	}
	return &WriteSortedMap{ReadSortedMap: rm, writeCursor: wc}, nil
}

func (m *WriteSortedMap) All() iter.Seq2[*WriteCursor, error] {
	return m.writeCursor.All()
}

func (m *WriteSortedMap) AllFrom(startKey []byte) iter.Seq2[*WriteCursor, error] {
	return writeIter(m.ReadSortedMap.AllFrom(startKey))
}

func (m *WriteSortedMap) AllFromIndex(startIndex int64) iter.Seq2[*WriteCursor, error] {
	return writeIter(m.ReadSortedMap.AllFromIndex(startIndex))
}

// String key methods

func (m *WriteSortedMap) Put(key string, data WriteableData) error {
	return m.PutByBytes([]byte(key), data)
}

func (m *WriteSortedMap) PutCursor(key string) (*WriteCursor, error) {
	return m.PutCursorByBytes([]byte(key))
}

func (m *WriteSortedMap) Remove(key string) (bool, error) {
	return m.RemoveByBytes([]byte(key))
}

// Byte-slice key methods

func (m *WriteSortedMap) PutByBytes(key []byte, data WriteableData) error {
	_, err := m.writeCursor.WritePath([]PathPart{
		SortedMapGetPart{Target: SortedMapGetValue{Key: key}},
		WriteData{Data: data},
	})
	return err
}

func (m *WriteSortedMap) PutCursorByBytes(key []byte) (*WriteCursor, error) {
	return m.writeCursor.WritePath([]PathPart{SortedMapGetPart{Target: SortedMapGetValue{Key: key}}})
}

func (m *WriteSortedMap) RemoveByBytes(key []byte) (bool, error) {
	_, err := m.writeCursor.WritePath([]PathPart{SortedMapRemovePart{Key: key}})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
