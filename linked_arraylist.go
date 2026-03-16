package xitdb

import "iter"

// ReadLinkedArrayList

type ReadLinkedArrayList struct {
	cursor *ReadCursor
}

func NewReadLinkedArrayList(cursor *ReadCursor) (*ReadLinkedArrayList, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagLinkedArrayList:
		return &ReadLinkedArrayList{cursor: cursor}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (a *ReadLinkedArrayList) Slot() Slot {
	return a.cursor.Slot()
}

func (a *ReadLinkedArrayList) Count() (int64, error) {
	return a.cursor.Count()
}

func (a *ReadLinkedArrayList) GetCursor(index int64) (*ReadCursor, error) {
	return a.cursor.ReadPath([]PathPart{LinkedArrayListGet{Index: index}})
}

func (a *ReadLinkedArrayList) GetSlot(index int64) (Slot, error) {
	return a.cursor.ReadPathSlot([]PathPart{LinkedArrayListGet{Index: index}})
}

func (a *ReadLinkedArrayList) All() iter.Seq2[*ReadCursor, error] {
	return a.cursor.All()
}

// WriteLinkedArrayList

type WriteLinkedArrayList struct {
	*ReadLinkedArrayList
	writeCursor *WriteCursor
}

func NewWriteLinkedArrayList(cursor *WriteCursor) (*WriteLinkedArrayList, error) {
	wc, err := cursor.WritePath([]PathPart{LinkedArrayListInit{}})
	if err != nil {
		return nil, err
	}
	ra, err := NewReadLinkedArrayList(wc.ReadCursor)
	if err != nil {
		return nil, err
	}
	return &WriteLinkedArrayList{ReadLinkedArrayList: ra, writeCursor: wc}, nil
}

func (a *WriteLinkedArrayList) Put(index int64, data WriteableData) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		LinkedArrayListGet{Index: index},
		WriteData{Data: data},
	})
	return err
}

func (a *WriteLinkedArrayList) PutCursor(index int64) (*WriteCursor, error) {
	return a.writeCursor.WritePath([]PathPart{LinkedArrayListGet{Index: index}})
}

func (a *WriteLinkedArrayList) Append(data WriteableData) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		LinkedArrayListAppend{},
		WriteData{Data: data},
	})
	return err
}

func (a *WriteLinkedArrayList) AppendCursor() (*WriteCursor, error) {
	return a.writeCursor.WritePath([]PathPart{LinkedArrayListAppend{}})
}

func (a *WriteLinkedArrayList) Slice(offset, size int64) error {
	_, err := a.writeCursor.WritePath([]PathPart{LinkedArrayListSlicePart{Offset: offset, Size: size}})
	return err
}

func (a *WriteLinkedArrayList) Concat(list Slot) error {
	_, err := a.writeCursor.WritePath([]PathPart{LinkedArrayListConcatPart{List: list}})
	return err
}

func (a *WriteLinkedArrayList) Insert(index int64, data WriteableData) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		LinkedArrayListInsertPart{Index: index},
		WriteData{Data: data},
	})
	return err
}

func (a *WriteLinkedArrayList) InsertCursor(index int64) (*WriteCursor, error) {
	return a.writeCursor.WritePath([]PathPart{LinkedArrayListInsertPart{Index: index}})
}

func (a *WriteLinkedArrayList) Remove(index int64) error {
	_, err := a.writeCursor.WritePath([]PathPart{LinkedArrayListRemovePart{Index: index}})
	return err
}

func (a *WriteLinkedArrayList) All() iter.Seq2[*WriteCursor, error] {
	return a.writeCursor.All()
}
