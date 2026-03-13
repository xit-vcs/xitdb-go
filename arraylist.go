package xitdb

import "iter"

// ReadArrayList

type ReadArrayList struct {
	cursor *ReadCursor
}

func NewReadArrayList(cursor *ReadCursor) (*ReadArrayList, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case TagNone, TagArrayList:
		return &ReadArrayList{cursor: cursor}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (a *ReadArrayList) GetSlot() Slot {
	return a.cursor.GetSlot()
}

func (a *ReadArrayList) Count() (int64, error) {
	return a.cursor.Count()
}

func (a *ReadArrayList) GetCursor(index int64) (*ReadCursor, error) {
	return a.cursor.ReadPath([]PathPart{ArrayListGet{Index: index}})
}

func (a *ReadArrayList) GetSlotAt(index int64) (*Slot, error) {
	return a.cursor.ReadPathSlot([]PathPart{ArrayListGet{Index: index}})
}

func (a *ReadArrayList) All() iter.Seq2[*ReadCursor, error] {
	return a.cursor.All()
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
		WriteDataPart{Data: data},
	})
	return err
}

func (a *WriteArrayList) PutCursor(index int64) (*WriteCursor, error) {
	return a.writeCursor.WritePath([]PathPart{ArrayListGet{Index: index}})
}

func (a *WriteArrayList) Append(data WriteableData) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		ArrayListAppend{},
		WriteDataPart{Data: data},
	})
	return err
}

func (a *WriteArrayList) AppendCursor() (*WriteCursor, error) {
	return a.writeCursor.WritePath([]PathPart{ArrayListAppend{}})
}

func (a *WriteArrayList) AppendContext(data WriteableData, fn ContextFunction) error {
	_, err := a.writeCursor.WritePath([]PathPart{
		ArrayListAppend{},
		WriteDataPart{Data: data},
		ContextPart{Function: fn},
	})
	return err
}

func (a *WriteArrayList) Slice(size int64) error {
	_, err := a.writeCursor.WritePath([]PathPart{ArrayListSlice{Size: size}})
	return err
}

func (a *WriteArrayList) All() iter.Seq2[*WriteCursor, error] {
	return a.writeCursor.All()
}
