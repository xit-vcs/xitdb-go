package xitdb

import (
	"errors"
	"iter"
)

// ReadSortedSet - a sorted set of byte-string keys (a SortedMap with no values).

type ReadSortedSet struct {
	Cursor *ReadCursor
}

func NewReadSortedSet(cursor *ReadCursor) (*ReadSortedSet, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagSortedMap, TagSortedSet:
		return &ReadSortedSet{Cursor: cursor}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (s *ReadSortedSet) Slot() Slot {
	return s.Cursor.Slot()
}

func (s *ReadSortedSet) Count() (int64, error) {
	return s.Cursor.Count()
}

func (s *ReadSortedSet) All() iter.Seq2[*ReadCursor, error] {
	return s.Cursor.All()
}

func (s *ReadSortedSet) AllFrom(startKey []byte) iter.Seq2[*ReadCursor, error] {
	cursor := s.Cursor
	return sortedIterSeq(func() (*CursorIterator, error) {
		return newSortedIterFromKey(cursor, startKey)
	})
}

func (s *ReadSortedSet) AllFromIndex(startIndex int64) iter.Seq2[*ReadCursor, error] {
	cursor := s.Cursor
	return sortedIterSeq(func() (*CursorIterator, error) {
		return newSortedIterFromIndex(cursor, startIndex)
	})
}

// GetIndexKeyValuePair returns the key/value pair at the given rank (negative counts from the end)
func (s *ReadSortedSet) GetIndexKeyValuePair(index int64) (*ReadKVPairCursor, error) {
	cursor, err := s.Cursor.ReadPath([]PathPart{SortedMapGetIndexPart{Index: index}})
	if err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, nil
	}
	return cursor.ReadKeyValuePair()
}

func (s *ReadSortedSet) Contains(key string) (bool, error) {
	return s.ContainsByBytes([]byte(key))
}

func (s *ReadSortedSet) ContainsByBytes(key []byte) (bool, error) {
	cursor, err := s.Cursor.ReadPath([]PathPart{SortedMapGetPart{Target: SortedMapGetKey{Key: key}}})
	if err != nil {
		return false, err
	}
	return cursor != nil, nil
}

func (s *ReadSortedSet) Rank(key string) (int64, error) {
	return s.RankByBytes([]byte(key))
}

// RankByBytes returns the number of keys strictly less than key
func (s *ReadSortedSet) RankByBytes(key []byte) (int64, error) {
	if s.Cursor.SlotPtr.Slot.Tag == TagNone {
		return 0, nil
	}
	if err := s.Cursor.DB.Core.SeekTo(s.Cursor.SlotPtr.Slot.Value); err != nil {
		return 0, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := s.Cursor.DB.Core.Read(headerBytes[:]); err != nil {
		return 0, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return 0, err
	}
	return s.Cursor.DB.sortedRank(header.RootPtr, key)
}

// WriteSortedSet

type WriteSortedSet struct {
	*ReadSortedSet
	writeCursor *WriteCursor
}

func NewWriteSortedSet(cursor *WriteCursor) (*WriteSortedSet, error) {
	wc, err := cursor.WritePath([]PathPart{SortedMapInitPart{Set: true}})
	if err != nil {
		return nil, err
	}
	rs, err := NewReadSortedSet(wc.ReadCursor)
	if err != nil {
		return nil, err
	}
	return &WriteSortedSet{ReadSortedSet: rs, writeCursor: wc}, nil
}

func (s *WriteSortedSet) All() iter.Seq2[*WriteCursor, error] {
	return s.writeCursor.All()
}

func (s *WriteSortedSet) AllFrom(startKey []byte) iter.Seq2[*WriteCursor, error] {
	return writeIter(s.ReadSortedSet.AllFrom(startKey))
}

func (s *WriteSortedSet) AllFromIndex(startIndex int64) iter.Seq2[*WriteCursor, error] {
	return writeIter(s.ReadSortedSet.AllFromIndex(startIndex))
}

func (s *WriteSortedSet) Put(key string) error {
	return s.PutByBytes([]byte(key))
}

func (s *WriteSortedSet) PutByBytes(key []byte) error {
	_, err := s.writeCursor.WritePath([]PathPart{SortedMapGetPart{Target: SortedMapGetKey{Key: key}}})
	return err
}

func (s *WriteSortedSet) Remove(key string) (bool, error) {
	return s.RemoveByBytes([]byte(key))
}

func (s *WriteSortedSet) RemoveByBytes(key []byte) (bool, error) {
	_, err := s.writeCursor.WritePath([]PathPart{SortedMapRemovePart{Key: key}})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
