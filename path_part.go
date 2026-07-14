package xitdb

import (
	"encoding/binary"
	"errors"
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
		} else if db.Header.Tag != TagArrayList {
			return SlotPointer{}, ErrUnexpectedTag
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
		// create an empty tree: a single empty leaf plus a header
		rootPtr, err := db.writeBTreeNode(BTreeNode{Kind: BTreeKindLeaf, Num: 0})
		if err != nil {
			return SlotPointer{}, err
		}
		headerPtr, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.SeekTo(headerPtr); err != nil {
			return SlotPointer{}, err
		}
		header := BTreeHeader{RootPtr: rootPtr, Size: 0}
		hb := header.ToBytes()
		if err := db.Core.Write(hb[:]); err != nil {
			return SlotPointer{}, err
		}
		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: headerPtr, Tag: TagLinkedArrayList}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	case TagLinkedArrayList:
		headerPtr := slotPtr.Slot.Value
		// copy the header into this transaction unless it was made in it, so past
		// moments still pointing at the old header are unaffected. b-tree nodes are
		// always appended, so only the header needs copying.
		if db.TxStart != nil {
			if headerPtr < *db.TxStart {
				if err := db.Core.SeekTo(headerPtr); err != nil {
					return SlotPointer{}, err
				}
				var headerBytes [BTreeHeaderLength]byte
				if err := db.Core.Read(headerBytes[:]); err != nil {
					return SlotPointer{}, err
				}
				newPtr, err := db.Core.Length()
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(newPtr); err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.Write(headerBytes[:]); err != nil {
					return SlotPointer{}, err
				}
				headerPtr = newPtr
			}
		} else if db.Header.Tag == TagArrayList {
			return SlotPointer{}, ErrExpectedTxStart
		}
		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: headerPtr, Tag: TagLinkedArrayList}}
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
	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}
	if index >= header.Size || index < -header.Size {
		return SlotPointer{}, ErrKeyNotFound
	}
	var rank int64
	if index < 0 {
		rank = header.Size - int64(math.Abs(float64(index)))
	} else {
		rank = index
	}

	if writeMode == ReadOnly {
		finalSlotPtr, err := db.readBTreeSlot(header.RootPtr, rank)
		if err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, finalSlotPtr)
	}

	// path-copy down to the value slot so the write is persistent
	writeSlot, err := db.btreeGetForWrite(header.RootPtr, rank)
	if err != nil {
		return SlotPointer{}, err
	}
	valuePosition := writeSlot.ValuePosition
	finalSlotPtr, err := db.readSlotPointer(writeMode, path, pathI+1, SlotPointer{Position: &valuePosition, Slot: writeSlot.Slot})
	if err != nil {
		return SlotPointer{}, err
	}
	// the header only needs rewriting if the root actually moved
	if writeSlot.NodePtr != header.RootPtr {
		if err := db.Core.SeekTo(headerPtr); err != nil {
			return SlotPointer{}, err
		}
		newHeader := BTreeHeader{RootPtr: writeSlot.NodePtr, Size: header.Size}
		nhb := newHeader.ToBytes()
		if err := db.Core.Write(nhb[:]); err != nil {
			return SlotPointer{}, err
		}
	}
	return finalSlotPtr, nil
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

	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	result, err := db.btreeInsert(header.RootPtr, header.Size)
	if err != nil {
		return SlotPointer{}, err
	}
	newRootPtr, err := db.btreeGrowRoot(result)
	if err != nil {
		return SlotPointer{}, err
	}

	// update the header before filling in the value, so that a failure in the
	// rest of the path leaves the tree and header consistent
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	newHeader := BTreeHeader{RootPtr: newRootPtr, Size: header.Size + 1}
	nhb := newHeader.ToBytes()
	if err := db.Core.Write(nhb[:]); err != nil {
		return SlotPointer{}, err
	}

	// fill in the value via the rest of the path
	valuePosition := result.ValuePosition
	return db.readSlotPointer(writeMode, path, pathI+1, SlotPointer{Position: &valuePosition, Slot: Slot{}})
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

	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	// bounds-checked without overflow (offset + size could wrap)
	if p.Offset > header.Size || p.Size > header.Size-p.Offset {
		return SlotPointer{}, ErrKeyNotFound
	}

	// slice = drop [0, offset) then keep [0, size) of what's left
	afterOffset, err := db.btreeSplit(header.RootPtr, p.Offset)
	if err != nil {
		return SlotPointer{}, err
	}
	sliced, err := db.btreeSplit(afterOffset.Right, p.Size)
	if err != nil {
		return SlotPointer{}, err
	}
	newRootPtr := sliced.Left

	// update the header before recursing into the rest of the path, so that a
	// failure there leaves the tree and header consistent
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	newHeader := BTreeHeader{RootPtr: newRootPtr, Size: p.Size}
	nhb := newHeader.ToBytes()
	if err := db.Core.Write(nhb[:]); err != nil {
		return SlotPointer{}, err
	}

	return db.readSlotPointer(writeMode, path, pathI+1, slotPtr)
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

	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytesA [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytesA[:]); err != nil {
		return SlotPointer{}, err
	}
	headerA, err := BTreeHeaderFromBytes(headerBytesA[:])
	if err != nil {
		return SlotPointer{}, err
	}

	if err := db.Core.SeekTo(p.List.Value); err != nil {
		return SlotPointer{}, err
	}
	var headerBytesB [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytesB[:]); err != nil {
		return SlotPointer{}, err
	}
	headerB, err := BTreeHeaderFromBytes(headerBytesB[:])
	if err != nil {
		return SlotPointer{}, err
	}

	// the join result shares subtrees with both operands (and the second operand stays
	// live), so freeze everything created so far: later in-place mutations will then
	// copy those nodes instead of overwriting a node that is still referenced elsewhere.
	txStart, err := db.Core.Length()
	if err != nil {
		return SlotPointer{}, err
	}
	db.TxStart = &txStart
	newRootPtr, err := db.btreeJoin(headerA.RootPtr, headerB.RootPtr)
	if err != nil {
		return SlotPointer{}, err
	}

	// update the header before recursing into the rest of the path, so that a
	// failure there leaves the tree and header consistent
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	newHeader := BTreeHeader{RootPtr: newRootPtr, Size: headerA.Size + headerB.Size}
	nhb := newHeader.ToBytes()
	if err := db.Core.Write(nhb[:]); err != nil {
		return SlotPointer{}, err
	}

	return db.readSlotPointer(writeMode, path, pathI+1, slotPtr)
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

	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	index := p.Index
	if index >= header.Size || index < -header.Size {
		return SlotPointer{}, ErrKeyNotFound
	}
	var rank int64
	if index < 0 {
		rank = header.Size - int64(math.Abs(float64(index)))
	} else {
		rank = index
	}

	result, err := db.btreeInsert(header.RootPtr, rank)
	if err != nil {
		return SlotPointer{}, err
	}
	newRootPtr, err := db.btreeGrowRoot(result)
	if err != nil {
		return SlotPointer{}, err
	}

	// update the header before filling in the value, so that a failure in the
	// rest of the path leaves the tree and header consistent
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	newHeader := BTreeHeader{RootPtr: newRootPtr, Size: header.Size + 1}
	nhb := newHeader.ToBytes()
	if err := db.Core.Write(nhb[:]); err != nil {
		return SlotPointer{}, err
	}

	valuePosition := result.ValuePosition
	return db.readSlotPointer(writeMode, path, pathI+1, SlotPointer{Position: &valuePosition, Slot: Slot{}})
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

	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	index := p.Index
	if index >= header.Size || index < -header.Size {
		return SlotPointer{}, ErrKeyNotFound
	}
	var rank int64
	if index < 0 {
		rank = header.Size - int64(math.Abs(float64(index)))
	} else {
		rank = index
	}

	// remove = join the parts before and after the removed element
	before, err := db.btreeSplit(header.RootPtr, rank)
	if err != nil {
		return SlotPointer{}, err
	}
	after, err := db.btreeSplit(before.Right, 1)
	if err != nil {
		return SlotPointer{}, err
	}
	newRootPtr, err := db.btreeJoin(before.Left, after.Right)
	if err != nil {
		return SlotPointer{}, err
	}

	// update the header before recursing into the rest of the path, so that a
	// failure there leaves the tree and header consistent
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	newHeader := BTreeHeader{RootPtr: newRootPtr, Size: header.Size - 1}
	nhb := newHeader.ToBytes()
	if err := db.Core.Write(nhb[:]); err != nil {
		return SlotPointer{}, err
	}

	return db.readSlotPointer(writeMode, path, pathI+1, slotPtr)
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
		} else {
			// map/set variants are interchangeable, but counted-ness must
			// match: counted layouts have an 8-byte count before the index
			// block, so misreading one as the other corrupts the database
			switch db.Header.Tag {
			case TagHashMap, TagHashSet:
				if p.Counted {
					return SlotPointer{}, ErrUnexpectedTag
				}
			case TagCountedHashMap, TagCountedHashSet:
				if !p.Counted {
					return SlotPointer{}, ErrUnexpectedTag
				}
			default:
				return SlotPointer{}, ErrUnexpectedTag
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
		if errors.Is(removeErr, ErrKeyNotFound) {
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

// SortedMapInit

type SortedMapInitPart struct {
	Set bool
}

func (p SortedMapInitPart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
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
	tag := TagSortedMap
	if p.Set {
		tag = TagSortedSet
	}

	switch slotPtr.Slot.Tag {
	case TagNone:
		rootPtr, err := db.writeSortedNode(SortedNode{Kind: BTreeKindLeaf, Num: 0})
		if err != nil {
			return SlotPointer{}, err
		}
		headerPtr, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.Core.SeekTo(headerPtr); err != nil {
			return SlotPointer{}, err
		}
		header := BTreeHeader{RootPtr: rootPtr, Size: 0}
		hb := header.ToBytes()
		if err := db.Core.Write(hb[:]); err != nil {
			return SlotPointer{}, err
		}
		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: headerPtr, Tag: tag}}
		if err := db.Core.SeekTo(position); err != nil {
			return SlotPointer{}, err
		}
		sb := nextSlotPtr.Slot.ToBytes()
		if err := db.Core.Write(sb[:]); err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, nextSlotPtr)

	case TagSortedMap, TagSortedSet:
		if slotPtr.Slot.Tag != tag {
			return SlotPointer{}, ErrUnexpectedTag
		}
		headerPtr := slotPtr.Slot.Value
		// copy the header into this transaction unless it was made in it
		if db.TxStart != nil {
			if headerPtr < *db.TxStart {
				if err := db.Core.SeekTo(headerPtr); err != nil {
					return SlotPointer{}, err
				}
				var headerBytes [BTreeHeaderLength]byte
				if err := db.Core.Read(headerBytes[:]); err != nil {
					return SlotPointer{}, err
				}
				newPtr, err := db.Core.Length()
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(newPtr); err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.Write(headerBytes[:]); err != nil {
					return SlotPointer{}, err
				}
				headerPtr = newPtr
			}
		} else if db.Header.Tag == TagArrayList {
			return SlotPointer{}, ErrExpectedTxStart
		}
		nextSlotPtr := SlotPointer{Position: &position, Slot: Slot{Value: headerPtr, Tag: tag}}
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

// SortedMapGet

type SortedMapGetPart struct {
	Target SortedMapGetTarget
}

func (p SortedMapGetPart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	switch slotPtr.Slot.Tag {
	case TagNone:
		return SlotPointer{}, ErrKeyNotFound
	case TagSortedMap, TagSortedSet:
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	key := p.Target.getKey()
	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	if writeMode == ReadOnly {
		found, err := db.sortedGet(header.RootPtr, key)
		if err != nil {
			return SlotPointer{}, err
		}
		if found == nil {
			return SlotPointer{}, ErrKeyNotFound
		}
		targetSlot, err := db.sortedTargetSlot(found.Slot.Value, p.Target)
		if err != nil {
			return SlotPointer{}, err
		}
		return db.readSlotPointer(writeMode, path, pathI+1, targetSlot)
	}

	result, err := db.sortedPut(header.RootPtr, key)
	if err != nil {
		return SlotPointer{}, err
	}
	newRootPtr, err := db.sortedGrowRoot(result)
	if err != nil {
		return SlotPointer{}, err
	}

	// update the header before filling in the value, so that a failure in the
	// rest of the path leaves the tree and header consistent (the entry exists
	// with an empty value) rather than inserted-but-uncounted
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	newSize := header.Size
	if result.Added {
		newSize++
	}
	newHeader := BTreeHeader{RootPtr: newRootPtr, Size: newSize}
	nhb := newHeader.ToBytes()
	if err := db.Core.Write(nhb[:]); err != nil {
		return SlotPointer{}, err
	}

	kvPos := result.ValuePosition - int64(db.Header.HashSize) - SlotLength
	targetSlot, err := db.sortedTargetSlot(kvPos, p.Target)
	if err != nil {
		return SlotPointer{}, err
	}
	return db.readSlotPointer(writeMode, path, pathI+1, targetSlot)
}

// SortedMapGetIndex

type SortedMapGetIndexPart struct {
	Index int64
}

func (p SortedMapGetIndexPart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadWrite {
		return SlotPointer{}, ErrWriteNotAllowed
	}

	switch slotPtr.Slot.Tag {
	case TagNone:
		return SlotPointer{}, ErrKeyNotFound
	case TagSortedMap, TagSortedSet:
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	index := p.Index
	if index >= header.Size || index < -header.Size {
		return SlotPointer{}, ErrKeyNotFound
	}
	var rank int64
	if index < 0 {
		rank = header.Size - int64(math.Abs(float64(index)))
	} else {
		rank = index
	}

	found, err := db.sortedGetByIndex(header.RootPtr, rank)
	if err != nil {
		return SlotPointer{}, err
	}
	// return the kv_pair entry so the caller can read key and value
	pos := found.Position
	targetSlot := SlotPointer{Position: &pos, Slot: found.Slot}
	return db.readSlotPointer(writeMode, path, pathI+1, targetSlot)
}

// SortedMapRemove

type SortedMapRemovePart struct {
	Key []byte
}

func (p SortedMapRemovePart) readSlotPointer(db *Database, isTopLevel bool, writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if writeMode == ReadOnly {
		return SlotPointer{}, ErrWriteNotAllowed
	}

	switch slotPtr.Slot.Tag {
	case TagNone:
		return SlotPointer{}, ErrKeyNotFound
	case TagSortedMap, TagSortedSet:
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	headerPtr := slotPtr.Slot.Value
	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return SlotPointer{}, err
	}

	result, err := db.sortedRemove(header.RootPtr, p.Key)
	if err != nil {
		return SlotPointer{}, err
	}
	if !result.Found {
		return SlotPointer{}, ErrKeyNotFound
	}

	if err := db.Core.SeekTo(headerPtr); err != nil {
		return SlotPointer{}, err
	}
	newHeader := BTreeHeader{RootPtr: result.NodePtr, Size: header.Size - 1}
	nhb := newHeader.ToBytes()
	if err := db.Core.Write(nhb[:]); err != nil {
		return SlotPointer{}, err
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
