package xitdb

import (
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"math"
)

type ReadCursor struct {
	SlotPtr SlotPointer
	DB      *Database
}

func (c *ReadCursor) Slot() Slot {
	return c.SlotPtr.Slot
}

func (c *ReadCursor) ReadPath(path []PathPart) (*ReadCursor, error) {
	slotPtr, err := c.DB.readSlotPointer(ReadOnly, path, 0, c.SlotPtr)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &ReadCursor{SlotPtr: slotPtr, DB: c.DB}, nil
}

func (c *ReadCursor) ReadPathSlot(path []PathPart) (Slot, error) {
	slotPtr, err := c.DB.readSlotPointer(ReadOnly, path, 0, c.SlotPtr)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return Slot{}, nil
		}
		return Slot{}, err
	}
	return slotPtr.Slot, nil
}

func (c *ReadCursor) ReadUint() (uint64, error) {
	if c.SlotPtr.Slot.Tag != TagUint {
		return 0, ErrUnexpectedTag
	}
	if c.SlotPtr.Slot.Value < 0 {
		return 0, ErrExpectedUnsignedLong
	}
	return uint64(c.SlotPtr.Slot.Value), nil
}

func (c *ReadCursor) ReadInt() (int64, error) {
	if c.SlotPtr.Slot.Tag != TagInt {
		return 0, ErrUnexpectedTag
	}
	return c.SlotPtr.Slot.Value, nil
}

func (c *ReadCursor) ReadFloat() (float64, error) {
	if c.SlotPtr.Slot.Tag != TagFloat {
		return 0, ErrUnexpectedTag
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(c.SlotPtr.Slot.Value))
	return math.Float64frombits(binary.BigEndian.Uint64(buf[:])), nil
}

func (c *ReadCursor) ReadBytes(maxSize int64) ([]byte, error) {
	obj, err := c.ReadBytesObject(maxSize)
	if err != nil {
		return nil, err
	}
	return obj.Value, nil
}

func (c *ReadCursor) ReadBytesObject(maxSize int64) (Bytes, error) {
	switch c.SlotPtr.Slot.Tag {
	case TagNone:
		return Bytes{Value: []byte{}}, nil
	case TagBytes:
		if err := c.DB.Core.SeekTo(c.SlotPtr.Slot.Value); err != nil {
			return Bytes{}, err
		}
		valueSize, err := readLong(c.DB.Core)
		if err != nil {
			return Bytes{}, err
		}
		if maxSize > 0 && valueSize > maxSize {
			return Bytes{}, ErrStreamTooLong
		}
		startPosition, err := c.DB.Core.Position()
		if err != nil {
			return Bytes{}, err
		}
		value := make([]byte, valueSize)
		if err := c.DB.Core.Read(value); err != nil {
			return Bytes{}, err
		}
		var formatTag []byte
		if c.SlotPtr.Slot.Full {
			if err := c.DB.Core.SeekTo(startPosition + valueSize); err != nil {
				return Bytes{}, err
			}
			formatTag = make([]byte, 2)
			if err := c.DB.Core.Read(formatTag); err != nil {
				return Bytes{}, err
			}
		}
		return Bytes{Value: value, FormatTag: formatTag}, nil
	case TagShortBytes:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(c.SlotPtr.Slot.Value))

		var totalSize int64 = 8
		if c.SlotPtr.Slot.Full {
			totalSize = 6
		}

		var valueSize int64
		for _, b := range buf {
			if b == 0 || valueSize == totalSize {
				break
			}
			valueSize++
		}

		if maxSize > 0 && valueSize > maxSize {
			return Bytes{}, ErrStreamTooLong
		}

		var formatTag []byte
		if c.SlotPtr.Slot.Full {
			formatTag = make([]byte, 2)
			copy(formatTag, buf[totalSize:totalSize+2])
		}

		value := make([]byte, valueSize)
		copy(value, buf[:valueSize])
		return Bytes{Value: value, FormatTag: formatTag}, nil
	default:
		return Bytes{}, ErrUnexpectedTag
	}
}

// ReadKVPairCursor

type ReadKVPairCursor struct {
	ValueCursor *ReadCursor
	KeyCursor   *ReadCursor
	Hash        []byte
}

func (c *ReadCursor) ReadKeyValuePair() (*ReadKVPairCursor, error) {
	if c.SlotPtr.Slot.Tag != TagKVPair {
		return nil, ErrUnexpectedTag
	}

	if err := c.DB.Core.SeekTo(c.SlotPtr.Slot.Value); err != nil {
		return nil, err
	}
	kvPairBytes := make([]byte, KeyValuePairLength(int(c.DB.Header.HashSize)))
	if err := c.DB.Core.Read(kvPairBytes); err != nil {
		return nil, err
	}
	kvPair := KeyValuePairFromBytes(kvPairBytes, int(c.DB.Header.HashSize))

	hashPos := c.SlotPtr.Slot.Value
	keySlotPos := hashPos + int64(c.DB.Header.HashSize)
	valueSlotPos := keySlotPos + int64(SlotLength)

	return &ReadKVPairCursor{
		ValueCursor: &ReadCursor{SlotPtr: SlotPointer{Position: &valueSlotPos, Slot: kvPair.ValueSlot}, DB: c.DB},
		KeyCursor:   &ReadCursor{SlotPtr: SlotPointer{Position: &keySlotPos, Slot: kvPair.KeySlot}, DB: c.DB},
		Hash:        kvPair.Hash,
	}, nil
}

// CursorReader - implements io.Reader

type CursorReader struct {
	parent           *ReadCursor
	size             int64
	startPosition    int64
	relativePosition int64
}

func (c *ReadCursor) Reader() (*CursorReader, error) {
	switch c.SlotPtr.Slot.Tag {
	case TagBytes:
		if err := c.DB.Core.SeekTo(c.SlotPtr.Slot.Value); err != nil {
			return nil, err
		}
		size, err := readLong(c.DB.Core)
		if err != nil {
			return nil, err
		}
		startPosition, err := c.DB.Core.Position()
		if err != nil {
			return nil, err
		}
		return &CursorReader{parent: c, size: size, startPosition: startPosition, relativePosition: 0}, nil
	case TagShortBytes:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(c.SlotPtr.Slot.Value))

		totalSize := 8
		if c.SlotPtr.Slot.Full {
			totalSize = 6
		}

		valueSize := 0
		for _, b := range buf {
			if b == 0 || valueSize == totalSize {
				break
			}
			valueSize++
		}

		startPosition := *c.SlotPtr.Position + 1
		return &CursorReader{parent: c, size: int64(valueSize), startPosition: startPosition, relativePosition: 0}, nil
	default:
		return nil, ErrUnexpectedTag
	}
}

func (r *CursorReader) Read(p []byte) (int, error) {
	if r.size < r.relativePosition {
		return 0, ErrEndOfStream
	}
	if err := r.parent.DB.Core.SeekTo(r.startPosition + r.relativePosition); err != nil {
		return 0, err
	}
	readSize := min(int64(len(p)), r.size-r.relativePosition)
	if readSize == 0 {
		return 0, io.EOF
	}
	if err := r.parent.DB.Core.Read(p[:readSize]); err != nil {
		return 0, err
	}
	r.relativePosition += readSize
	return int(readSize), nil
}

func (r *CursorReader) ReadFully(buf []byte) error {
	if r.size < r.relativePosition || r.size-r.relativePosition < int64(len(buf)) {
		return ErrEndOfStream
	}
	if err := r.parent.DB.Core.SeekTo(r.startPosition + r.relativePosition); err != nil {
		return err
	}
	if err := r.parent.DB.Core.Read(buf); err != nil {
		return err
	}
	r.relativePosition += int64(len(buf))
	return nil
}

func (r *CursorReader) ReadByte() (byte, error) {
	var buf [1]byte
	if err := r.ReadFully(buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *CursorReader) ReadShort() (int16, error) {
	var buf [2]byte
	if err := r.ReadFully(buf[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(buf[:])), nil
}

func (r *CursorReader) ReadInt32() (int32, error) {
	var buf [4]byte
	if err := r.ReadFully(buf[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf[:])), nil
}

func (r *CursorReader) ReadLong() (int64, error) {
	var buf [8]byte
	if err := r.ReadFully(buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf[:])), nil
}

func (r *CursorReader) SeekTo(position int64) error {
	if position > r.size {
		return ErrInvalidOffset
	}
	r.relativePosition = position
	return nil
}

func (r *CursorReader) ReadAll() ([]byte, error) {
	remaining := r.size - r.relativePosition
	buf := make([]byte, remaining)
	if err := r.ReadFully(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Count

func (c *ReadCursor) Count() (int64, error) {
	switch c.SlotPtr.Slot.Tag {
	case TagNone:
		return 0, nil
	case TagArrayList:
		if err := c.DB.Core.SeekTo(c.SlotPtr.Slot.Value); err != nil {
			return 0, err
		}
		var headerBytes [ArrayListHeaderLength]byte
		if err := c.DB.Core.Read(headerBytes[:]); err != nil {
			return 0, err
		}
		header, err := ArrayListHeaderFromBytes(headerBytes[:])
		if err != nil {
			return 0, err
		}
		return header.Size, nil
	case TagLinkedArrayList:
		if err := c.DB.Core.SeekTo(c.SlotPtr.Slot.Value); err != nil {
			return 0, err
		}
		var headerBytes [LinkedArrayListHeaderLength]byte
		if err := c.DB.Core.Read(headerBytes[:]); err != nil {
			return 0, err
		}
		header, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
		if err != nil {
			return 0, err
		}
		return header.Size, nil
	case TagBytes:
		if err := c.DB.Core.SeekTo(c.SlotPtr.Slot.Value); err != nil {
			return 0, err
		}
		return readLong(c.DB.Core)
	case TagShortBytes:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(c.SlotPtr.Slot.Value))

		totalSize := 8
		if c.SlotPtr.Slot.Full {
			totalSize = 6
		}

		size := 0
		for _, b := range buf {
			if b == 0 || size == totalSize {
				break
			}
			size++
		}
		return int64(size), nil
	case TagCountedHashMap, TagCountedHashSet:
		if err := c.DB.Core.SeekTo(c.SlotPtr.Slot.Value); err != nil {
			return 0, err
		}
		return readLong(c.DB.Core)
	default:
		return 0, ErrUnexpectedTag
	}
}

// CursorIterator - stack-based tree traversal

type iterLevel struct {
	position int64
	block    [SlotCount]Slot
	index    byte
}

type CursorIterator struct {
	cursor          *ReadCursor
	size            int64
	index           int64
	stack           []iterLevel
	nextCursorMaybe *ReadCursor // only used when iterating over hash maps
}

func newCursorIterator(cursor *ReadCursor) (*CursorIterator, error) {
	it := &CursorIterator{cursor: cursor}

	switch cursor.SlotPtr.Slot.Tag {
	case TagNone:
		it.size = 0
		it.index = 0
	case TagArrayList:
		position := cursor.SlotPtr.Slot.Value
		if err := cursor.DB.Core.SeekTo(position); err != nil {
			return nil, err
		}
		var headerBytes [ArrayListHeaderLength]byte
		if err := cursor.DB.Core.Read(headerBytes[:]); err != nil {
			return nil, err
		}
		header, err := ArrayListHeaderFromBytes(headerBytes[:])
		if err != nil {
			return nil, err
		}
		count, err := cursor.Count()
		if err != nil {
			return nil, err
		}
		it.size = count
		it.index = 0
		level, err := initIterStack(cursor, header.Ptr, IndexBlockSize)
		if err != nil {
			return nil, err
		}
		it.stack = []iterLevel{level}
	case TagLinkedArrayList:
		position := cursor.SlotPtr.Slot.Value
		if err := cursor.DB.Core.SeekTo(position); err != nil {
			return nil, err
		}
		var headerBytes [LinkedArrayListHeaderLength]byte
		if err := cursor.DB.Core.Read(headerBytes[:]); err != nil {
			return nil, err
		}
		header, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
		if err != nil {
			return nil, err
		}
		count, err := cursor.Count()
		if err != nil {
			return nil, err
		}
		it.size = count
		it.index = 0
		level, err := initIterStack(cursor, header.Ptr, LinkedArrayListIndexBlockSize)
		if err != nil {
			return nil, err
		}
		it.stack = []iterLevel{level}
	case TagHashMap, TagHashSet:
		it.size = 0
		it.index = 0
		level, err := initIterStack(cursor, cursor.SlotPtr.Slot.Value, IndexBlockSize)
		if err != nil {
			return nil, err
		}
		it.stack = []iterLevel{level}
	case TagCountedHashMap, TagCountedHashSet:
		it.size = 0
		it.index = 0
		level, err := initIterStack(cursor, cursor.SlotPtr.Slot.Value+8, IndexBlockSize)
		if err != nil {
			return nil, err
		}
		it.stack = []iterLevel{level}
	default:
		return nil, ErrUnexpectedTag
	}

	return it, nil
}

func initIterStack(cursor *ReadCursor, position int64, blockSize int) (iterLevel, error) {
	if err := cursor.DB.Core.SeekTo(position); err != nil {
		return iterLevel{}, err
	}
	indexBlockBytes := make([]byte, blockSize)
	if err := cursor.DB.Core.Read(indexBlockBytes); err != nil {
		return iterLevel{}, err
	}
	var block [SlotCount]Slot
	slotSize := blockSize / SlotCount
	for i := 0; i < SlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], indexBlockBytes[i*slotSize:i*slotSize+SlotLength])
		block[i] = SlotFromBytes(sb)
	}
	return iterLevel{position: position, block: block, index: 0}, nil
}

func (it *CursorIterator) nextInternal(blockSize int) (*ReadCursor, error) {
	for len(it.stack) > 0 {
		level := &it.stack[len(it.stack)-1]
		if int(level.index) == len(level.block) {
			it.stack = it.stack[:len(it.stack)-1]
			if len(it.stack) > 0 {
				it.stack[len(it.stack)-1].index++
			}
			continue
		}

		nextSlot := level.block[level.index]
		if nextSlot.Tag == TagIndex {
			nextPos := nextSlot.Value
			if err := it.cursor.DB.Core.SeekTo(nextPos); err != nil {
				return nil, err
			}
			indexBlockBytes := make([]byte, blockSize)
			if err := it.cursor.DB.Core.Read(indexBlockBytes); err != nil {
				return nil, err
			}
			var block [SlotCount]Slot
			slotSize := blockSize / SlotCount
			for i := 0; i < SlotCount; i++ {
				var sb [SlotLength]byte
				copy(sb[:], indexBlockBytes[i*slotSize:i*slotSize+SlotLength])
				block[i] = SlotFromBytes(sb)
			}
			it.stack = append(it.stack, iterLevel{position: nextPos, block: block, index: 0})
			continue
		}

		level.index++
		if !nextSlot.Empty() {
			position := level.position + int64(level.index-1)*int64(SlotLength)
			return &ReadCursor{
				SlotPtr: SlotPointer{Position: &position, Slot: nextSlot},
				DB:      it.cursor.DB,
			}, nil
		}
	}
	return nil, nil
}

// All returns an iterator for range-over-func (Go 1.23+)

func (c *ReadCursor) All() iter.Seq2[*ReadCursor, error] {
	return func(yield func(*ReadCursor, error) bool) {
		it, err := newCursorIterator(c)
		if err != nil {
			yield(nil, err)
			return
		}

		switch c.SlotPtr.Slot.Tag {
		case TagNone:
			return
		case TagArrayList:
			for it.index < it.size {
				it.index++
				cursor, err := it.nextInternal(IndexBlockSize)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					return
				}
				if cursor != nil {
					if !yield(cursor, nil) {
						return
					}
				}
			}
		case TagLinkedArrayList:
			for it.index < it.size {
				it.index++
				cursor, err := it.nextInternal(LinkedArrayListIndexBlockSize)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					return
				}
				if cursor != nil {
					if !yield(cursor, nil) {
						return
					}
				}
			}
		case TagHashMap, TagHashSet, TagCountedHashMap, TagCountedHashSet:
			for {
				cursor, err := it.nextInternal(IndexBlockSize)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					return
				}
				if cursor == nil {
					return
				}
				if !yield(cursor, nil) {
					return
				}
			}
		}
	}
}
