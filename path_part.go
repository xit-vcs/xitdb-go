package xitdb

import (
	"encoding/binary"
	"math"
)

type PathPart interface {
	readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error)
}

// ArrayListInit

type ArrayListInit struct{}

func (p ArrayListInit) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}

	if isTopLevel {
		if db.Header.Tag == TagNone {
			if err := db.Core.SeekTo(int64(DatabaseStart)); err != nil {
				return SlotPointer{}, err
			}
			arrayListPtr := int64(DatabaseStart) + int64(TopLevelArrayListHeaderLength)
			tlHeader := TopLevelArrayListHeader{
				FileSize: 0,
				Parent:   ArrayListHeader{Ptr: arrayListPtr, Size: 0},
			}
			b := tlHeader.ToBytes()
			if err := db.Core.Write(b[:]); err != nil {
				return SlotPointer{}, err
			}
			if err := db.Core.Write(make([]byte, IndexBlockSize)); err != nil {
				return SlotPointer{}, err
			}
			if err := db.Core.SeekTo(0); err != nil {
				return SlotPointer{}, err
			}
			db.Header = db.Header.WithTag(TagArrayList)
			if err := db.Header.Write(db.Core); err != nil {
				return SlotPointer{}, err
			}
		}
		nextSlotPtr := slotPtr.WithSlot(slotPtr.Slot.WithTag(TagArrayList))
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)
	}

	if slotPtr.Position == nil {
		return SlotPointer{}, ErrCursorNotWriteable
	}
	position := *slotPtr.Position

	switch slotPtr.Slot.Tag {
	case TagNone:
		arrayListStart, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.SeekTo(arrayListStart); err != nil {
			return SlotPointer{}, err
		}
		arrayListPtr := arrayListStart + int64(ArrayListHeaderLength)
		alHeader := ArrayListHeader{Ptr: arrayListPtr, Size: 0}
		b := alHeader.ToBytes()
		if err := db.Core.Write(b[:]); err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.Write(make([]byte, IndexBlockSize)); err != nil {
			return SlotPointer{}, err
		}
		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: arrayListStart, Tag: TagArrayList}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	case TagArrayList:
		arrayListStart := slotPtr.Slot.Value

		if db.TxStart != nil {
			if arrayListStart < *db.TxStart {
				if err := db.Core.SeekTo(arrayListStart); err != nil {
					return SlotPointer{}, err
				}
				var headerBytes [ArrayListHeaderLength]byte
				if err := db.Core.Read(headerBytes[:]); err != nil {
					return SlotPointer{}, err
				}
				header, err := ArrayListHeaderFromBytes(headerBytes[:])
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(header.Ptr); err != nil {
					return SlotPointer{}, err
				}
				arrayListIndexBlock := make([]byte, IndexBlockSize)
				if err := db.Core.Read(arrayListIndexBlock); err != nil {
					return SlotPointer{}, err
				}
				newStart, err := db.Core.Length()
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(newStart); err != nil {
					return SlotPointer{}, err
				}
				nextArrayListPtr := newStart + int64(ArrayListHeaderLength)
				header = header.WithPtr(nextArrayListPtr)
				hb := header.ToBytes()
				if err := db.Core.Write(hb[:]); err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.Write(arrayListIndexBlock); err != nil {
					return SlotPointer{}, err
				}
				arrayListStart = newStart
			}
		} else if db.Header.Tag == TagArrayList {
			return SlotPointer{}, ErrExpectedTxStart
		}

		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: arrayListStart, Tag: TagArrayList}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	default:
		return SlotPointer{}, ErrUnexpectedTag
	}
}

// ArrayListGet

type ArrayListGet struct {
	Index int64
}

func (p ArrayListGet) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	tag := slotPtr.Slot.Tag
	if isTopLevel {
		tag = db.Header.Tag
	}
	switch tag {
	case TagNone:
		return SlotPointer{}, ErrKeyNotFound
	case TagArrayList:
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value
	index := p.Index

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [ArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := ArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}
	if index >= header.Size || index < -header.Size {
		return SlotPointer{}, ErrKeyNotFound
	}

	var key int64
	if index < 0 {
		key = header.Size - int64(math.Abs(float64(index)))
	} else {
		key = index
	}
	lastKey := header.Size - 1
	var shift byte
	if lastKey < SlotCount {
		shift = 0
	} else {
		shift = byte(math.Log(float64(lastKey)) / math.Log(float64(SlotCount)))
	}
	finalSlotPtr, err := db.readArrayListSlot(header.Ptr, key, shift, writeMode, isTopLevel)
	if err != nil {
		return SlotPointer{}, err
	}

	return db.readSlotPointer(writeMode, path, pathI+1, finalSlotPtr)
}

// ArrayListAppend

type ArrayListAppend struct{}

func (p ArrayListAppend) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}

	tag := slotPtr.Slot.Tag
	if isTopLevel {
		tag = db.Header.Tag
	}
	if tag != TagArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [ArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	origHeader, err := ArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	appendResult, err := db.readArrayListSlotAppend(origHeader, writeMode, isTopLevel)
	if err != nil {
		return SlotPointer{}, err
	}
	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, appendResult.SlotPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	if isTopLevel {
		if err := db.Core.Flush(); err != nil {
			return SlotPointer{}, err
		}
		fileSize, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		tlHeader := TopLevelArrayListHeader{FileSize: fileSize, Parent: appendResult.Header}
		if err := db.Core.SeekTo(nextArrayListStart); err != nil {
			return SlotPointer{}, err
		}
		b := tlHeader.ToBytes()
		if err := db.Core.Write(b[:]); err != nil {
			return SlotPointer{}, err
		}
	} else {
		if err := db.Core.SeekTo(nextArrayListStart); err != nil {
			return SlotPointer{}, err
		}
		b := appendResult.Header.ToBytes()
		if err := db.Core.Write(b[:]); err != nil {
			return SlotPointer{}, err
		}
	}

	return finalSlotPtr, nil
}

// ArrayListSlice

type ArrayListSlice struct {
	Size int64
}

func (p ArrayListSlice) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if slotPtr.Slot.Tag != TagArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value
	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [ArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	origHeader, err := ArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	sliceHeader, err := db.readArrayListSlice(origHeader, p.Size)
	if err != nil {
		return SlotPointer{}, err
	}
	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, slotPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	b := sliceHeader.ToBytes()
	if err := db.Core.Write(b[:]); err != nil {
		return SlotPointer{}, err
	}

	return finalSlotPtr, nil
}

// LinkedArrayListInit

type LinkedArrayListInit struct{}

func (p LinkedArrayListInit) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if isTopLevel {
		return SlotPointer{}, ErrInvalidTopLevelType
	}
	if slotPtr.Position == nil {
		return SlotPointer{}, ErrCursorNotWriteable
	}
	position := *slotPtr.Position

	switch slotPtr.Slot.Tag {
	case TagNone:
		arrayListStart, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.SeekTo(arrayListStart); err != nil {
			return SlotPointer{}, err
		}
		arrayListPtr := arrayListStart + int64(LinkedArrayListHeaderLength)
		laHeader := LinkedArrayListHeader{Shift: 0, Ptr: arrayListPtr, Size: 0}
		b := laHeader.ToBytes()
		if err := db.Core.Write(b[:]); err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.Write(make([]byte, LinkedArrayListIndexBlockSize)); err != nil {
			return SlotPointer{}, err
		}
		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: arrayListStart, Tag: TagLinkedArrayList}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	case TagLinkedArrayList:
		arrayListStart := slotPtr.Slot.Value

		if db.TxStart != nil {
			if arrayListStart < *db.TxStart {
				if err := db.Core.SeekTo(arrayListStart); err != nil {
					return SlotPointer{}, err
				}
				var headerBytes [LinkedArrayListHeaderLength]byte
				if err := db.Core.Read(headerBytes[:]); err != nil {
					return SlotPointer{}, err
				}
				header, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(header.Ptr); err != nil {
					return SlotPointer{}, err
				}
				arrayListIndexBlock := make([]byte, LinkedArrayListIndexBlockSize)
				if err := db.Core.Read(arrayListIndexBlock); err != nil {
					return SlotPointer{}, err
				}
				newStart, err := db.Core.Length()
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(newStart); err != nil {
					return SlotPointer{}, err
				}
				nextArrayListPtr := newStart + int64(LinkedArrayListHeaderLength)
				header = header.WithPtr(nextArrayListPtr)
				hb := header.ToBytes()
				if err := db.Core.Write(hb[:]); err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.Write(arrayListIndexBlock); err != nil {
					return SlotPointer{}, err
				}
				arrayListStart = newStart
			}
		} else if db.Header.Tag == TagArrayList {
			return SlotPointer{}, ErrExpectedTxStart
		}

		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: arrayListStart, Tag: TagLinkedArrayList}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	default:
		return SlotPointer{}, ErrUnexpectedTag
	}
}

// LinkedArrayListGet

type LinkedArrayListGet struct {
	Index int64
}

func (p LinkedArrayListGet) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	switch slotPtr.Slot.Tag {
	case TagNone:
		return SlotPointer{}, ErrKeyNotFound
	case TagLinkedArrayList:
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	index := p.Index
	if err := db.Core.SeekTo(slotPtr.Slot.Value); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [LinkedArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}
	if index >= header.Size || index < -header.Size {
		return SlotPointer{}, ErrKeyNotFound
	}

	var key int64
	if index < 0 {
		key = header.Size - int64(math.Abs(float64(index)))
	} else {
		key = index
	}
	finalSlotPtr, err := db.readLinkedArrayListSlot(header.Ptr, key, header.Shift, writeMode, isTopLevel)
	if err != nil {
		return SlotPointer{}, err
	}

	return db.readSlotPointer(writeMode, path, pathI+1, finalSlotPtr.SlotPtr)
}

// LinkedArrayListAppend

type LinkedArrayListAppend struct{}

func (p LinkedArrayListAppend) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if slotPtr.Slot.Tag != TagLinkedArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value
	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [LinkedArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	origHeader, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	appendResult, err := db.readLinkedArrayListSlotAppend(origHeader, writeMode, isTopLevel)
	if err != nil {
		return SlotPointer{}, err
	}
	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, appendResult.SlotPtr.SlotPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	b := appendResult.Header.ToBytes()
	if err := db.Core.Write(b[:]); err != nil {
		return SlotPointer{}, err
	}

	return finalSlotPtr, nil
}

// LinkedArrayListSlicePart

type LinkedArrayListSlicePart struct {
	Offset int64
	Size   int64
}

func (p LinkedArrayListSlicePart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if slotPtr.Slot.Tag != TagLinkedArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value
	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [LinkedArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	origHeader, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	sliceHeader, err := db.readLinkedArrayListSlice(origHeader, p.Offset, p.Size)
	if err != nil {
		return SlotPointer{}, err
	}
	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, slotPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	b := sliceHeader.ToBytes()
	if err := db.Core.Write(b[:]); err != nil {
		return SlotPointer{}, err
	}

	return finalSlotPtr, nil
}

// LinkedArrayListConcat

type LinkedArrayListConcatPart struct {
	List Slot
}

func (p LinkedArrayListConcatPart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if slotPtr.Slot.Tag != TagLinkedArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}
	if p.List.Tag != TagLinkedArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytesA [LinkedArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytesA[:]); err != nil {
		return SlotPointer{}, err
	}
	headerA, err := LinkedArrayListHeaderFromBytes(headerBytesA[:])
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(p.List.Value); err != nil {
		return SlotPointer{}, err
	}
	var headerBytesB [LinkedArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytesB[:]); err != nil {
		return SlotPointer{}, err
	}
	headerB, err := LinkedArrayListHeaderFromBytes(headerBytesB[:])
	if err != nil {
		return SlotPointer{}, err
	}

	concatHeader, err := db.readLinkedArrayListConcat(headerA, headerB)
	if err != nil {
		return SlotPointer{}, err
	}
	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, slotPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	b := concatHeader.ToBytes()
	if err := db.Core.Write(b[:]); err != nil {
		return SlotPointer{}, err
	}

	return finalSlotPtr, nil
}

// LinkedArrayListInsert

type LinkedArrayListInsertPart struct {
	Index int64
}

func (p LinkedArrayListInsertPart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if slotPtr.Slot.Tag != TagLinkedArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value
	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [LinkedArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	origHeader, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	index := p.Index
	if index >= origHeader.Size || index < -origHeader.Size {
		return SlotPointer{}, ErrKeyNotFound
	}
	var key int64
	if index < 0 {
		key = origHeader.Size - int64(math.Abs(float64(index)))
	} else {
		key = index
	}

	headerA, err := db.readLinkedArrayListSlice(origHeader, 0, key)
	if err != nil {
		return SlotPointer{}, err
	}
	headerB, err := db.readLinkedArrayListSlice(origHeader, key, origHeader.Size-key)
	if err != nil {
		return SlotPointer{}, err
	}

	appendResult, err := db.readLinkedArrayListSlotAppend(headerA, writeMode, isTopLevel)
	if err != nil {
		return SlotPointer{}, err
	}

	concatHeader, err := db.readLinkedArrayListConcat(appendResult.Header, headerB)
	if err != nil {
		return SlotPointer{}, err
	}

	nextSlotPtr, err := db.readLinkedArrayListSlot(concatHeader.Ptr, key, concatHeader.Shift, ReadOnly, isTopLevel)
	if err != nil {
		return SlotPointer{}, err
	}

	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr.SlotPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	b := concatHeader.ToBytes()
	if err := db.Core.Write(b[:]); err != nil {
		return SlotPointer{}, err
	}

	return finalSlotPtr, nil
}

// LinkedArrayListRemove

type LinkedArrayListRemovePart struct {
	Index int64
}

func (p LinkedArrayListRemovePart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if slotPtr.Slot.Tag != TagLinkedArrayList {
		return SlotPointer{}, ErrUnexpectedTag
	}

	nextArrayListStart := slotPtr.Slot.Value
	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [LinkedArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	origHeader, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	index := p.Index
	if index >= origHeader.Size || index < -origHeader.Size {
		return SlotPointer{}, ErrKeyNotFound
	}
	var key int64
	if index < 0 {
		key = origHeader.Size - int64(math.Abs(float64(index)))
	} else {
		key = index
	}

	headerA, err := db.readLinkedArrayListSlice(origHeader, 0, key)
	if err != nil {
		return SlotPointer{}, err
	}
	headerB, err := db.readLinkedArrayListSlice(origHeader, key+1, origHeader.Size-(key+1))
	if err != nil {
		return SlotPointer{}, err
	}

	concatHeader, err := db.readLinkedArrayListConcat(headerA, headerB)
	if err != nil {
		return SlotPointer{}, err
	}

	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, slotPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(nextArrayListStart); err != nil {
		return SlotPointer{}, err
	}
	b := concatHeader.ToBytes()
	if err := db.Core.Write(b[:]); err != nil {
		return SlotPointer{}, err
	}

	return finalSlotPtr, nil
}

// HashMapInit

type HashMapInitPart struct {
	Counted bool
	Set     bool
}

func (p HashMapInitPart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}

	var tag Tag
	if p.Counted {
		if p.Set {
			tag = TagCountedHashSet
		} else {
			tag = TagCountedHashMap
		}
	} else {
		if p.Set {
			tag = TagHashSet
		} else {
			tag = TagHashMap
		}
	}

	if isTopLevel {
		if db.Header.Tag == TagNone {
			if err := db.Core.SeekTo(int64(DatabaseStart)); err != nil {
				return SlotPointer{}, err
			}
			if p.Counted {
				if err := writeLong(db.Core, 0); err != nil {
					return SlotPointer{}, err
				}
			}
			if err := db.Core.Write(make([]byte, IndexBlockSize)); err != nil {
				return SlotPointer{}, err
			}
			if err := db.Core.SeekTo(0); err != nil {
				return SlotPointer{}, err
			}
			db.Header = db.Header.WithTag(tag)
			if err := db.Header.Write(db.Core); err != nil {
				return SlotPointer{}, err
			}
		}
		nextSlotPtr := slotPtr.WithSlot(slotPtr.Slot.WithTag(tag))
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)
	}

	if slotPtr.Position == nil {
		return SlotPointer{}, ErrCursorNotWriteable
	}
	position := *slotPtr.Position

	switch slotPtr.Slot.Tag {
	case TagNone:
		mapStart, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.SeekTo(mapStart); err != nil {
			return SlotPointer{}, err
		}
		if p.Counted {
			if err := writeLong(db.Core, 0); err != nil {
				return SlotPointer{}, err
			}
		}
		if err := db.Core.Write(make([]byte, IndexBlockSize)); err != nil {
			return SlotPointer{}, err
		}
		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: mapStart, Tag: tag}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	case TagHashMap, TagHashSet, TagCountedHashMap, TagCountedHashSet:
		if p.Counted {
			switch slotPtr.Slot.Tag {
			case TagCountedHashMap, TagCountedHashSet:
			default:
				return SlotPointer{}, ErrUnexpectedTag
			}
		} else {
			switch slotPtr.Slot.Tag {
			case TagHashMap, TagHashSet:
			default:
				return SlotPointer{}, ErrUnexpectedTag
			}
		}

		mapStart := slotPtr.Slot.Value

		if db.TxStart != nil {
			if mapStart < *db.TxStart {
				if err := db.Core.SeekTo(mapStart); err != nil {
					return SlotPointer{}, err
				}
				var mapCountMaybe *int64
				if p.Counted {
					v, err := readLong(db.Core)
					if err != nil {
						return SlotPointer{}, err
					}
					mapCountMaybe = &v
				}
				mapIndexBlock := make([]byte, IndexBlockSize)
				if err := db.Core.Read(mapIndexBlock); err != nil {
					return SlotPointer{}, err
				}
				newStart, err := db.Core.Length()
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(newStart); err != nil {
					return SlotPointer{}, err
				}
				if mapCountMaybe != nil {
					if err := writeLong(db.Core, *mapCountMaybe); err != nil {
						return SlotPointer{}, err
					}
				}
				if err := db.Core.Write(mapIndexBlock); err != nil {
					return SlotPointer{}, err
				}
				mapStart = newStart
			}
		} else if db.Header.Tag == TagArrayList {
			return SlotPointer{}, ErrExpectedTxStart
		}

		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: mapStart, Tag: tag}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	default:
		return SlotPointer{}, ErrUnexpectedTag
	}
}

// HashMapGet

type HashMapGetPart struct {
	Target HashMapGetTarget
}

func (p HashMapGetPart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	counted := false
	switch slotPtr.Slot.Tag {
	case TagNone:
		return SlotPointer{}, ErrKeyNotFound
	case TagHashMap, TagHashSet:
	case TagCountedHashMap, TagCountedHashSet:
		counted = true
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	var indexPos int64
	if counted {
		indexPos = slotPtr.Slot.Value + 8
	} else {
		indexPos = slotPtr.Slot.Value
	}

	hash, err := db.checkHashTarget(p.Target)
	if err != nil {
		return SlotPointer{}, err
	}

	res, err := db.readMapSlot(indexPos, hash, 0, writeMode, isTopLevel, p.Target)
	if err != nil {
		return SlotPointer{}, err
	}

	if writeMode == ReadWrite && counted && res.IsEmpty {
		if err := db.Core.SeekTo(slotPtr.Slot.Value); err != nil {
			return SlotPointer{}, err
		}
		mapCount, err := readLong(db.Core)
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.SeekTo(slotPtr.Slot.Value); err != nil {
			return SlotPointer{}, err
		}
		if err := writeLong(db.Core, mapCount+1); err != nil {
			return SlotPointer{}, err
		}
	}

	return db.readSlotPointer(writeMode, path, pathI+1, res.SlotPtr)
}

// HashMapRemove

type HashMapRemovePart struct {
	Hash []byte
}

func (p HashMapRemovePart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}

	counted := false
	switch slotPtr.Slot.Tag {
	case TagNone:
		return SlotPointer{}, ErrKeyNotFound
	case TagHashMap, TagHashSet:
	case TagCountedHashMap, TagCountedHashSet:
		counted = true
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	var indexPos int64
	if counted {
		indexPos = slotPtr.Slot.Value + 8
	} else {
		indexPos = slotPtr.Slot.Value
	}

	hash, err := db.checkHash(p.Hash)
	if err != nil {
		return SlotPointer{}, err
	}

	keyFound := true
	_, removeErr := db.removeMapSlot(indexPos, hash, 0, isTopLevel)
	if removeErr != nil {
		if removeErr == ErrKeyNotFound {
			keyFound = false
		} else {
			return SlotPointer{}, removeErr
		}
	}

	if writeMode == ReadWrite && counted && keyFound {
		if err := db.Core.SeekTo(slotPtr.Slot.Value); err != nil {
			return SlotPointer{}, err
		}
		mapCount, err := readLong(db.Core)
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.SeekTo(slotPtr.Slot.Value); err != nil {
			return SlotPointer{}, err
		}
		if err := writeLong(db.Core, mapCount-1); err != nil {
			return SlotPointer{}, err
		}
	}

	if !keyFound {
		return SlotPointer{}, ErrKeyNotFound
	}

	return slotPtr, nil
}

// WriteData

type WriteData struct {
	Data WriteableData
}

func (p WriteData) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if slotPtr.Position == nil {
		return SlotPointer{}, ErrCursorNotWriteable
	}
	position := *slotPtr.Position

	var slot Slot

	switch data := p.Data.(type) {
	case nil:
		slot = Slot{}
	case Slot:
		slot = data
	case Uint:
		slot = Slot{Value: int64(data.Value), Tag: TagUint}
	case Int:
		slot = Slot{Value: data.Value, Tag: TagInt}
	case Float:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(data.Value))
		slot = Slot{Value: int64(binary.BigEndian.Uint64(buf[:])), Tag: TagFloat}
	case Bytes:
		if data.FormatTag != nil && len(data.FormatTag) != 2 {
			return SlotPointer{}, ErrInvalidFormatTagSize
		}
		if data.IsShort() {
			var buf [8]byte
			copy(buf[:], data.Value)
			if data.FormatTag != nil {
				copy(buf[6:], data.FormatTag)
			}
			slot = Slot{Value: int64(binary.BigEndian.Uint64(buf[:])), Tag: TagShortBytes, Full: data.FormatTag != nil}
		} else {
			nextCursor := &WriteCursor{ReadCursor: &ReadCursor{SlotPtr: slotPtr, DB: db}}
			cursorWriter, err := nextCursor.Writer()
			if err != nil {
				return SlotPointer{}, err
			}
			cursorWriter.FormatTag = data.FormatTag
			if _, err := cursorWriter.Write(data.Value); err != nil {
				return SlotPointer{}, err
			}
			if err := cursorWriter.Finish(); err != nil {
				return SlotPointer{}, err
			}
			slot = cursorWriter.slot
		}
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	if slot.Tag == TagNone {
		slot = slot.WithFull(true)
	}

	if err := db.Core.SeekTo(position); err != nil {
		return SlotPointer{}, err
	}
	sb := slot.ToBytes()
	if err := db.Core.Write(sb[:]); err != nil {
		return SlotPointer{}, err
	}

	nextSlotPtr := SlotPointer{Position: slotPtr.Position, Slot: slot}
	return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)
}

// Context

type Context struct {
	Function ContextFunction
}

func (p Context) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}
	if pathI != len(path)-1 {
		return SlotPointer{}, ErrPathPartMustBeAtEnd
	}

	nextCursor := &WriteCursor{ReadCursor: &ReadCursor{SlotPtr: slotPtr, DB: db}}
	err := p.Function(nextCursor)
	if err != nil {
		// since an error occurred, there may be inaccessible junk at the end of the db
		db.truncate()
		return SlotPointer{}, err
	}
	return nextCursor.SlotPtr, nil
}
