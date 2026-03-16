package xitdb

import (
	"iter"
)

type WriteCursor struct {
	*ReadCursor
}

func (c *WriteCursor) WritePath(path []PathPart) (*WriteCursor, error) {
	slotPtr, err := c.DB.readSlotPointer(ReadWrite, path, 0, c.SlotPtr)
	if err != nil {
		return nil, err
	}
	if c.DB.TxStart == nil {
		if err := c.DB.Core.Sync(); err != nil {
			return nil, err
		}
	}
	return &WriteCursor{ReadCursor: &ReadCursor{SlotPtr: slotPtr, DB: c.DB}}, nil
}

func (c *WriteCursor) Write(data WriteableData) error {
	cursor, err := c.WritePath([]PathPart{WriteDataPart{Data: data}})
	if err != nil {
		return err
	}
	c.SlotPtr = cursor.SlotPtr
	return nil
}

func (c *WriteCursor) WriteIfEmpty(data WriteableData) error {
	if c.SlotPtr.Slot.Empty() {
		return c.Write(data)
	}
	return nil
}

// WriteKVPairCursor

type WriteKVPairCursor struct {
	ValueCursor *WriteCursor
	KeyCursor   *WriteCursor
	Hash        []byte
}

func (c *WriteCursor) ReadKeyValuePair() (*WriteKVPairCursor, error) {
	readKVP, err := c.ReadCursor.ReadKeyValuePair()
	if err != nil {
		return nil, err
	}
	return &WriteKVPairCursor{
		ValueCursor: &WriteCursor{ReadCursor: readKVP.ValueCursor},
		KeyCursor:   &WriteCursor{ReadCursor: readKVP.KeyCursor},
		Hash:        readKVP.Hash,
	}, nil
}

// CursorWriter

type CursorWriter struct {
	parent           *WriteCursor
	size             int64
	slot             Slot
	startPosition    int64
	relativePosition int64
	formatTag        []byte
}

func (c *WriteCursor) Writer() (*CursorWriter, error) {
	ptrPos, err := c.DB.Core.Length()
	if err != nil {
		return nil, err
	}
	if err := c.DB.Core.SeekTo(ptrPos); err != nil {
		return nil, err
	}
	if err := writeLong(c.DB.Core, 0); err != nil {
		return nil, err
	}
	startPosition, err := c.DB.Core.Length()
	if err != nil {
		return nil, err
	}
	return &CursorWriter{
		parent:           c,
		size:             0,
		slot:             Slot{Value: ptrPos, Tag: TagBytes},
		startPosition:    startPosition,
		relativePosition: 0,
	}, nil
}

func (w *CursorWriter) Write(p []byte) (int, error) {
	if w.size < w.relativePosition {
		return 0, ErrEndOfStream
	}
	if err := w.parent.DB.Core.SeekTo(w.startPosition + w.relativePosition); err != nil {
		return 0, err
	}
	if err := w.parent.DB.Core.Write(p); err != nil {
		return 0, err
	}
	w.relativePosition += int64(len(p))
	if w.relativePosition > w.size {
		w.size = w.relativePosition
	}
	return len(p), nil
}

func (w *CursorWriter) Finish() error {
	if w.formatTag != nil {
		w.slot = w.slot.WithFull(true)
		formatTagPos, err := w.parent.DB.Core.Length()
		if err != nil {
			return err
		}
		if err := w.parent.DB.Core.SeekTo(formatTagPos); err != nil {
			return err
		}
		if w.startPosition+w.size != formatTagPos {
			return ErrUnexpectedWriterPosition
		}
		if err := w.parent.DB.Core.Write(w.formatTag); err != nil {
			return err
		}
	}

	if err := w.parent.DB.Core.SeekTo(w.slot.Value); err != nil {
		return err
	}
	if err := writeLong(w.parent.DB.Core, w.size); err != nil {
		return err
	}

	if w.parent.SlotPtr.Position == nil {
		return ErrCursorNotWriteable
	}
	position := *w.parent.SlotPtr.Position
	if err := w.parent.DB.Core.SeekTo(position); err != nil {
		return err
	}
	sb := w.slot.ToBytes()
	if err := w.parent.DB.Core.Write(sb[:]); err != nil {
		return err
	}

	w.parent.SlotPtr = w.parent.SlotPtr.WithSlot(w.slot)
	return nil
}

func (w *CursorWriter) SeekTo(position int64) {
	if position <= w.size {
		w.relativePosition = position
	}
}

// WriteCursor All() - shadows ReadCursor.All() to return WriteCursors

func (c *WriteCursor) All() iter.Seq2[*WriteCursor, error] {
	return func(yield func(*WriteCursor, error) bool) {
		for rc, err := range c.ReadCursor.All() {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				return
			}
			wc := &WriteCursor{ReadCursor: rc}
			if !yield(wc, nil) {
				return
			}
		}
	}
}
