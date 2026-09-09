package xitdb

type WriteCursor struct {
	*ReadCursor
	transaction *transaction
}

func newWriteCursor(slotPtr SlotPointer, db *Database) *WriteCursor {
	cursor := &WriteCursor{ReadCursor: &ReadCursor{SlotPtr: slotPtr, DB: db}}
	if slotPtr.Position != nil {
		cursor.transaction = db.transaction
	}
	return cursor
}

// require the active transaction and a writable slot
func (c *WriteCursor) checkWritable() error {
	if c.transaction != nil && c.transaction != c.DB.transaction {
		return ErrExpiredTransaction
	}
	if c.SlotPtr.Position != nil && c.DB.Header.Tag == TagArrayList && c.transaction == nil {
		return ErrExpectedTxStart
	}
	return c.DB.checkFrozenSlot(c.SlotPtr)
}

// reload after freezing because copy-on-write may change where the slot points
func (c *WriteCursor) reloadSlot() error {
	if c.transaction != nil && c.transaction.frozenAt != nil && c.SlotPtr.Position != nil {
		if err := c.DB.Core.SeekTo(*c.SlotPtr.Position); err != nil {
			return err
		}
		var buf [SlotLength]byte
		if err := c.DB.Core.Read(buf[:]); err != nil {
			return err
		}
		c.SlotPtr = c.SlotPtr.WithSlot(SlotFromBytes(buf))
	}
	return nil
}

func (c *WriteCursor) WritePath(path []PathPart) (*WriteCursor, error) {
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	// nested top-level writes could commit before the outer transaction ends
	if c.DB.transaction != nil && c.SlotPtr.Position == nil && len(path) > 0 {
		return nil, ErrNestedTopLevelWrite
	}
	initializesHistory := false
	if len(path) > 0 {
		switch path[0].(type) {
		case ArrayListInit, *ArrayListInit:
			initializesHistory = true
		}
	}
	startsTransaction := c.DB.transaction == nil && c.SlotPtr.Position == nil &&
		(c.DB.Header.Tag == TagArrayList || initializesHistory)
	if startsTransaction {
		c.DB.transaction = &transaction{}
		defer func() { c.DB.transaction = nil }()
	}
	err := c.reloadSlot()
	var slotPtr SlotPointer
	if err == nil {
		slotPtr, err = c.DB.readSlotPointer(ReadWrite, path, 0, c.SlotPtr)
	}
	if err != nil {
		// only truncate when the error escapes the outer write.
		// a nested callback's caller may still commit its work.
		if c.DB.TxStart == nil {
			_ = c.DB.truncate()
		}
		return nil, err
	}
	if c.DB.TxStart == nil {
		if err := c.DB.Core.Sync(); err != nil {
			return nil, err
		}
	}
	if err := c.reloadSlot(); err != nil {
		return nil, err
	}
	cursor := newWriteCursor(slotPtr, c.DB)
	if err := cursor.reloadSlot(); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (c *WriteCursor) Write(data WriteableData) error {
	cursor, err := c.WritePath([]PathPart{WriteData{Data: data}})
	if err != nil {
		return err
	}
	c.SlotPtr = cursor.SlotPtr
	return nil
}

func (c *WriteCursor) WriteIfEmpty(data WriteableData) error {
	if err := c.checkWritable(); err != nil {
		return err
	}
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
		ValueCursor: &WriteCursor{ReadCursor: readKVP.ValueCursor, transaction: c.transaction},
		KeyCursor:   &WriteCursor{ReadCursor: readKVP.KeyCursor, transaction: c.transaction},
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
	FormatTag        []byte
}

func (c *WriteCursor) Writer() (*CursorWriter, error) {
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
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
	if err := w.checkWritable(); err != nil {
		return 0, err
	}
	if w.size < w.relativePosition {
		return 0, ErrEndOfStream
	}
	newPosition := w.relativePosition + int64(len(p))

	// another allocation may now follow this byte array.
	// extending it would overwrite that allocation.
	if newPosition > w.size {
		end, err := w.parent.DB.Core.Length()
		if err != nil {
			return 0, err
		}
		if end != w.startPosition+w.size {
			return 0, ErrUnexpectedWriterPosition
		}
	}

	if err := w.parent.DB.Core.SeekTo(w.startPosition + w.relativePosition); err != nil {
		return 0, err
	}
	if err := w.parent.DB.Core.Write(p); err != nil {
		return 0, err
	}
	w.relativePosition = newPosition
	if w.relativePosition > w.size {
		w.size = w.relativePosition
	}
	return len(p), nil
}

func (w *CursorWriter) Finish() error {
	if err := w.checkWritable(); err != nil {
		return err
	}
	if w.FormatTag != nil {
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
		if err := w.parent.DB.Core.Write(w.FormatTag); err != nil {
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

// validate the parent cursor and reject writes to frozen bytes
func (w *CursorWriter) checkWritable() error {
	if err := w.parent.checkWritable(); err != nil {
		return err
	}
	active := w.parent.DB.transaction
	if active != nil && active.frozenAt != nil && w.slot.Value < *active.frozenAt {
		return ErrFrozenBytes
	}
	return nil
}

func (w *CursorWriter) SeekTo(position int64) {
	if position <= w.size {
		w.relativePosition = position
	}
}
