package xitdb

import "iter"

// ReadArrayList

type ReadArrayList struct {
	Cursor *ReadCursor
}

func NewReadArrayList(cursor *ReadCursor) (*ReadArrayList, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagArrayList:
		return &ReadArrayList{Cursor: cursor}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (a *ReadArrayList) Slot() Slot {
	return a.Cursor.Slot()
}

func (a *ReadArrayList) Count() (int64, error) {
	return a.Cursor.Count()
}

func (a *ReadArrayList) GetCursor(index int64) (*ReadCursor, error) {
	return a.Cursor.ReadPath([]PathPart{ArrayListGet{Index: index}})
}

func (a *ReadArrayList) GetSlot(index int64) (Slot, error) {
	return a.Cursor.ReadPathSlot([]PathPart{ArrayListGet{Index: index}})
}

func (a *ReadArrayList) All() iter.Seq2[*ReadCursor, error] {
	return a.Cursor.All()
}

// AllFrom iterates starting at the given index, seeking straight to it
// instead of walking from the front. negative indexes count from the end.
func (a *ReadArrayList) AllFrom(index int64) iter.Seq2[*ReadCursor, error] {
	cursor := a.Cursor
	return iterSeqFrom(func() (*CursorIterator, error) {
		return newArrayListIterFromIndex(cursor, index)
	}, 0)
}

// WriteArrayList

type WriteArrayList struct {
	*ReadArrayList
	writeCursor *WriteCursor
}

func NewWriteArrayList(cursor *WriteCursor) (*WriteArrayList, error) {
	wc, err := cursor.WritePath([]PathPart{ArrayListInit{}})
	if err != nil {
		return nil, err
	}
	ra, err := NewReadArrayList(wc.ReadCursor)
	if err != nil {
		return nil, err
	}
	return &WriteArrayList{ReadArrayList: ra, writeCursor: wc}, nil
}

func (a *WriteArrayList) Put(index int64, data WriteableData) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		ArrayListGet{Index: index},
		WriteData{Data: data},
	})
	return err
}

func (a *WriteArrayList) PutCursor(index int64) (*WriteCursor, error) {
	return a.writeCursor.WritePath([]PathPart{ArrayListGet{Index: index}})
}

func (a *WriteArrayList) Append(data WriteableData) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		ArrayListAppend{},
		WriteData{Data: data},
	})
	return err
}

func (a *WriteArrayList) AppendCursor() (*WriteCursor, error) {
	return a.writeCursor.WritePath([]PathPart{ArrayListAppend{}})
}

func (a *WriteArrayList) AppendContext(data WriteableData, fn ContextFunction) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		ArrayListAppend{},
		WriteData{Data: data},
		Context{Function: fn},
	})
	return err
}

func (a *WriteArrayList) Slice(size int64) error {
	_, err := a.writeCursor.WritePath([]PathPart{ArrayListSlice{Size: size}})
	return err
}
