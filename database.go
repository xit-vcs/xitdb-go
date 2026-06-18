package xitdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"math/big"
)

const (
	Version                        uint16 = 0
	DatabaseStart                        = HeaderLength
	BitCount                             = 4
	SlotCount                            = 1 << BitCount
	Mask                           int64 = SlotCount - 1
	IndexBlockSize                       = SlotLength * SlotCount
	MaxBranchLength                      = 16
	// b-tree (backs LinkedArrayList): nodes hold up to BTreeSlotCount entries
	BTreeSlotCount                       = SlotCount
	BTreeSplitCount                      = (BTreeSlotCount + 1) / 2
	BTreeNodeHeaderSize                  = 2
	BTreeLeafBlockSize                   = BTreeNodeHeaderSize + SlotLength*BTreeSlotCount
	BTreeBranchBlockSize                 = BTreeNodeHeaderSize + (SlotLength+8)*BTreeSlotCount
)

var (
	MagicNumber = [3]byte{'x', 'i', 't'}
	BigMask     = big.NewInt(Mask)
)

type WriteMode int

const (
	ReadOnly WriteMode = iota
	ReadWrite
)

// Header

const HeaderLength = 12

type Header struct {
	HashID      uint32
	HashSize    uint16
	Version     uint16
	Tag         Tag
	MagicNumber [3]byte
}

func (h Header) ToBytes() [HeaderLength]byte {
	var buf [HeaderLength]byte
	copy(buf[0:3], h.MagicNumber[:])
	buf[3] = byte(h.Tag)
	binary.BigEndian.PutUint16(buf[4:6], h.Version)
	binary.BigEndian.PutUint16(buf[6:8], h.HashSize)
	binary.BigEndian.PutUint32(buf[8:12], h.HashID)
	return buf
}

func ReadHeader(c Core) (Header, error) {
	var magicNumber [3]byte
	if err := c.Read(magicNumber[:]); err != nil {
		return Header{}, err
	}
	tagByte, err := readByte_(c)
	if err != nil {
		return Header{}, err
	}
	tag := Tag(tagByte & 0b0111_1111)
	version, err := readUint16(c)
	if err != nil {
		return Header{}, err
	}
	hashSize, err := readUint16(c)
	if err != nil {
		return Header{}, err
	}
	hashID, err := readUint32(c)
	if err != nil {
		return Header{}, err
	}
	return Header{
		HashID:      hashID,
		HashSize:    hashSize,
		Version:     version,
		Tag:         tag,
		MagicNumber: magicNumber,
	}, nil
}

func (h Header) Write(c Core) error {
	b := h.ToBytes()
	return c.Write(b[:])
}

func (h Header) Validate() error {
	if h.MagicNumber != MagicNumber {
		return ErrInvalidDatabase
	}
	if h.Version > Version {
		return ErrInvalidVersion
	}
	return nil
}

func (h Header) WithTag(tag Tag) Header {
	return Header{
		HashID:      h.HashID,
		HashSize:    h.HashSize,
		Version:     h.Version,
		Tag:         tag,
		MagicNumber: h.MagicNumber,
	}
}

// ArrayListHeader

const ArrayListHeaderLength = 16

type ArrayListHeader struct {
	Ptr  int64
	Size int64
}

func (h ArrayListHeader) ToBytes() [ArrayListHeaderLength]byte {
	var buf [ArrayListHeaderLength]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(h.Size))
	binary.BigEndian.PutUint64(buf[8:16], uint64(h.Ptr))
	return buf
}

func ArrayListHeaderFromBytes(b []byte) (ArrayListHeader, error) {
	size := int64(binary.BigEndian.Uint64(b[0:8]))
	ptr := int64(binary.BigEndian.Uint64(b[8:16]))
	if size < 0 {
		return ArrayListHeader{}, ErrExpectedUnsignedLong
	}
	if ptr < 0 {
		return ArrayListHeader{}, ErrExpectedUnsignedLong
	}
	return ArrayListHeader{Ptr: ptr, Size: size}, nil
}

func (h ArrayListHeader) WithPtr(ptr int64) ArrayListHeader {
	return ArrayListHeader{Ptr: ptr, Size: h.Size}
}

// TopLevelArrayListHeader

const TopLevelArrayListHeaderLength = 8 + ArrayListHeaderLength

type TopLevelArrayListHeader struct {
	FileSize int64
	Parent   ArrayListHeader
}

func (h TopLevelArrayListHeader) ToBytes() [TopLevelArrayListHeaderLength]byte {
	var buf [TopLevelArrayListHeaderLength]byte
	parent := h.Parent.ToBytes()
	copy(buf[0:ArrayListHeaderLength], parent[:])
	binary.BigEndian.PutUint64(buf[ArrayListHeaderLength:], uint64(h.FileSize))
	return buf
}

// BTreeHeader: a root pointer plus the element count (backs LinkedArrayList)

const BTreeHeaderLength = 16

type BTreeHeader struct {
	RootPtr int64
	Size    int64
}

func (h BTreeHeader) ToBytes() [BTreeHeaderLength]byte {
	var buf [BTreeHeaderLength]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(h.Size))
	binary.BigEndian.PutUint64(buf[8:16], uint64(h.RootPtr))
	return buf
}

func BTreeHeaderFromBytes(b []byte) (BTreeHeader, error) {
	size := int64(binary.BigEndian.Uint64(b[0:8]))
	rootPtr := int64(binary.BigEndian.Uint64(b[8:16]))
	if size < 0 {
		return BTreeHeader{}, ErrExpectedUnsignedLong
	}
	if rootPtr < 0 {
		return BTreeHeader{}, ErrExpectedUnsignedLong
	}
	return BTreeHeader{RootPtr: rootPtr, Size: size}, nil
}

// KeyValuePair

type KeyValuePair struct {
	ValueSlot Slot
	KeySlot   Slot
	Hash      []byte
}

func KeyValuePairLength(hashSize int) int {
	return hashSize + SlotLength*2
}

func (kvp KeyValuePair) ToBytes() []byte {
	buf := make([]byte, len(kvp.Hash)+SlotLength*2)
	copy(buf, kvp.Hash)
	keyBytes := kvp.KeySlot.ToBytes()
	copy(buf[len(kvp.Hash):], keyBytes[:])
	valueBytes := kvp.ValueSlot.ToBytes()
	copy(buf[len(kvp.Hash)+SlotLength:], valueBytes[:])
	return buf
}

func KeyValuePairFromBytes(b []byte, hashSize int) KeyValuePair {
	hash := make([]byte, hashSize)
	copy(hash, b[:hashSize])
	var keySlotBytes [SlotLength]byte
	copy(keySlotBytes[:], b[hashSize:hashSize+SlotLength])
	keySlot := SlotFromBytes(keySlotBytes)
	var valueSlotBytes [SlotLength]byte
	copy(valueSlotBytes[:], b[hashSize+SlotLength:hashSize+SlotLength*2])
	valueSlot := SlotFromBytes(valueSlotBytes)
	return KeyValuePair{ValueSlot: valueSlot, KeySlot: keySlot, Hash: hash}
}

// BTreeNode: a leaf holds value slots; a branch holds child slots (.index) plus a
// per-child u64 subtree count.

type BTreeNodeKind byte

const (
	BTreeKindLeaf   BTreeNodeKind = 0
	BTreeKindBranch BTreeNodeKind = 1
)

type BTreeNode struct {
	Kind     BTreeNodeKind
	Num      int
	Values   [BTreeSlotCount]Slot  // leaf
	Children [BTreeSlotCount]Slot  // branch
	Counts   [BTreeSlotCount]int64 // branch
}

func (n *BTreeNode) SubtreeCount() int64 {
	if n.Kind == BTreeKindLeaf {
		return int64(n.Num)
	}
	var total int64
	for i := 0; i < n.Num; i++ {
		total += n.Counts[i]
	}
	return total
}

// a node pointer plus the element count of its subtree (the right sibling of a split)
type BTreeNodeRef struct {
	NodePtr int64
	Count   int64
}

type BTreeInsertResult struct {
	NodePtr       int64
	Count         int64
	ValuePosition int64
	Split         *BTreeNodeRef
}

type BTreeWriteSlot struct {
	NodePtr       int64
	ValuePosition int64
	Slot          Slot
}

type BTreeJoinResult struct {
	NodePtr int64
	Count   int64
	Split   *BTreeNodeRef
}

type BTreeSplitResult struct {
	Left  int64
	Right int64
}

// HashMapGetTarget

type HashMapGetTarget interface {
	hashMapGetTarget()
	getHash() []byte
}

type HashMapGetKVPair struct{ Hash []byte }
type HashMapGetKey struct{ Hash []byte }
type HashMapGetValue struct{ Hash []byte }

func (HashMapGetKVPair) hashMapGetTarget() {}
func (HashMapGetKey) hashMapGetTarget()    {}
func (HashMapGetValue) hashMapGetTarget()  {}
func (h HashMapGetKVPair) getHash() []byte { return h.Hash }
func (h HashMapGetKey) getHash() []byte    { return h.Hash }
func (h HashMapGetValue) getHash() []byte  { return h.Hash }

// HashMapGetResult

type HashMapGetResult struct {
	SlotPtr SlotPointer
	IsEmpty bool
}

// ArrayListAppendResult

type ArrayListAppendResult struct {
	Header  ArrayListHeader
	SlotPtr SlotPointer
}

// ContextFunction

type ContextFunction func(cursor *WriteCursor) error

// Database

type Database struct {
	Core   Core
	hash   hash.Hash
	Header Header
	TxStart *int64
}

func NewDatabase(core Core, hasher Hasher) (*Database, error) {
	db := &Database{
		Core: core,
		hash: hasher.Hash,
	}

	if err := core.SeekTo(0); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}
	length, err := core.Length()
	if err != nil {
		return nil, fmt.Errorf("length: %w", err)
	}

	if length == 0 {
		digestLen := uint16(db.hash.Size())
		db.Header = Header{
			HashID:      hasher.ID,
			HashSize:    digestLen,
			Version:     Version,
			Tag:         TagNone,
			MagicNumber: MagicNumber,
		}
		if err := db.Header.Write(core); err != nil {
			return nil, fmt.Errorf("write header: %w", err)
		}
		if err := core.Flush(); err != nil {
			return nil, fmt.Errorf("flush: %w", err)
		}
	} else {
		header, err := ReadHeader(core)
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}
		if err := header.Validate(); err != nil {
			return nil, fmt.Errorf("validate header: %w", err)
		}
		digestLen := uint16(db.hash.Size())
		if header.HashSize != digestLen {
			return nil, ErrInvalidHashSize
		}
		db.Header = header
		if err := db.truncate(); err != nil {
			return nil, fmt.Errorf("truncate: %w", err)
		}
	}

	db.TxStart = nil
	return db, nil
}

func (db *Database) digest(data []byte) []byte {
	db.hash.Reset()
	db.hash.Write(data)
	return db.hash.Sum(nil)
}

func (db *Database) RootCursor() *WriteCursor {
	// if the header tag is none, try re-reading it.
    // this may be necessary if the database was initialized on a different thread.
	if db.Header.Tag == TagNone {
		if err := db.Core.SeekTo(0); err == nil {
			if header, err := ReadHeader(db.Core); err == nil {
				db.Header = header
			}
		}
	}
	rc := &ReadCursor{
		SlotPtr: SlotPointer{Position: nil, Slot: Slot{Value: int64(DatabaseStart), Tag: db.Header.Tag}},
		DB:      db,
	}
	return &WriteCursor{ReadCursor: rc}
}

func (db *Database) Freeze() error {
	if db.TxStart != nil {
		length, err := db.Core.Length()
		if err != nil {
			return err
		}
		db.TxStart = &length
		return nil
	}
	return ErrExpectedTxStart
}

func (db *Database) Compact(targetCore Core) (*Database, error) {
	offsetMap := make(map[int64]int64)
	hasher := Hasher{Hash: db.hash, ID: db.Header.HashID}
	target, err := NewDatabase(targetCore, hasher)
	if err != nil {
		return nil, fmt.Errorf("init target: %w", err)
	}

	if db.Header.Tag == TagNone {
		return target, nil
	}
	if db.Header.Tag != TagArrayList {
		return nil, ErrUnexpectedTag
	}

	// read source's top-level ArrayListHeader
	if err := db.Core.SeekTo(int64(DatabaseStart)); err != nil {
		return nil, fmt.Errorf("read source header: %w", err)
	}
	var headerBytes [ArrayListHeaderLength]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return nil, fmt.Errorf("read source header: %w", err)
	}
	sourceHeader, err := ArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return nil, fmt.Errorf("read source header: %w", err)
	}
	if sourceHeader.Size == 0 {
		return target, nil
	}

	// read the last moment's slot
	lastKey := sourceHeader.Size - 1
	var shift byte
	if lastKey < SlotCount {
		shift = 0
	} else {
		shift = byte(math.Log(float64(lastKey)) / math.Log(float64(SlotCount)))
	}
	lastSlotPtr, err := db.readArrayListSlot(sourceHeader.Ptr, lastKey, shift, ReadOnly, true)
	if err != nil {
		return nil, fmt.Errorf("read last slot: %w", err)
	}
	momentSlot := lastSlotPtr.Slot

	// write TopLevelArrayListHeader + root index block to target
	if err := target.Core.SeekTo(int64(DatabaseStart)); err != nil {
		return nil, fmt.Errorf("write target header: %w", err)
	}
	targetArrayListPtr := int64(DatabaseStart) + int64(TopLevelArrayListHeaderLength)
	tlHeader := TopLevelArrayListHeader{
		FileSize: 0,
		Parent:   ArrayListHeader{Ptr: targetArrayListPtr, Size: 1},
	}
	tlBytes := tlHeader.ToBytes()
	if err := target.Core.Write(tlBytes[:]); err != nil {
		return nil, fmt.Errorf("write target header: %w", err)
	}
	if err := target.Core.Write(make([]byte, IndexBlockSize)); err != nil {
		return nil, fmt.Errorf("write target index block: %w", err)
	}

	// recursively remap the moment slot
	remappedMoment, err := remapSlot(db.Core, target.Core, db.Header.HashSize, offsetMap, momentSlot)
	if err != nil {
		return nil, fmt.Errorf("remap: %w", err)
	}

	// write remapped moment slot into position 0 of target's root index block
	if err := target.Core.SeekTo(targetArrayListPtr); err != nil {
		return nil, fmt.Errorf("write remapped slot: %w", err)
	}
	remappedBytes := remappedMoment.ToBytes()
	if err := target.Core.Write(remappedBytes[:]); err != nil {
		return nil, fmt.Errorf("write remapped slot: %w", err)
	}

	// update target's DatabaseHeader tag
	target.Header = target.Header.WithTag(TagArrayList)
	if err := target.Core.SeekTo(0); err != nil {
		return nil, fmt.Errorf("write target db header: %w", err)
	}
	if err := target.Header.Write(target.Core); err != nil {
		return nil, fmt.Errorf("write target db header: %w", err)
	}

	// flush, update file_size, flush again
	if err := target.Core.Flush(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	fileSize, err := target.Core.Length()
	if err != nil {
		return nil, fmt.Errorf("get file size: %w", err)
	}
	if err := target.Core.SeekTo(int64(DatabaseStart) + int64(ArrayListHeaderLength)); err != nil {
		return nil, fmt.Errorf("write file size: %w", err)
	}
	if err := writeLong(target.Core, fileSize); err != nil {
		return nil, fmt.Errorf("write file size: %w", err)
	}
	if err := target.Core.Flush(); err != nil {
		return nil, fmt.Errorf("final flush: %w", err)
	}

	return target, nil
}

// truncate

func (db *Database) truncate() error {
	if db.Header.Tag != TagArrayList {
		return nil
	}

	rc := &ReadCursor{
		SlotPtr: SlotPointer{Position: nil, Slot: Slot{Value: int64(DatabaseStart), Tag: db.Header.Tag}},
		DB:      db,
	}
	wc := &WriteCursor{ReadCursor: rc}
	listSize, err := wc.Count()
	if err != nil {
		return err
	}
	if listSize == 0 {
		return nil
	}

	if err := db.Core.SeekTo(int64(DatabaseStart) + int64(ArrayListHeaderLength)); err != nil {
		return err
	}
	headerFileSize, err := readLong(db.Core)
	if err != nil {
		return err
	}
	if headerFileSize == 0 {
		return nil
	}

	fileSize, err := db.Core.Length()
	if err != nil {
		return err
	}
	if fileSize == headerFileSize {
		return nil
	}

	// ignore error because the file may be open in read-only mode
	_ = db.Core.SetLength(headerFileSize)
	return nil
}

// checkHash

func (db *Database) checkHash(hash []byte) ([]byte, error) {
	if len(hash) != int(db.Header.HashSize) {
		return nil, ErrInvalidHashSize
	}
	return hash, nil
}

func (db *Database) checkHashTarget(target HashMapGetTarget) ([]byte, error) {
	return db.checkHash(target.getHash())
}

func checkLong(n int64) (int64, error) {
	if n < 0 {
		return 0, ErrExpectedUnsignedLong
	}
	return n, nil
}

// readSlotPointer - the central path-traversal method

func (db *Database) readSlotPointer(writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if pathI == len(path) {
		if writeMode == ReadOnly && slotPtr.Slot.Tag == TagNone {
			return SlotPointer{}, ErrKeyNotFound
		}
		return slotPtr, nil
	}
	part := path[pathI]

	isTopLevel := slotPtr.Slot.Value == int64(DatabaseStart)

	isTxStart := isTopLevel && db.Header.Tag == TagArrayList && db.TxStart == nil
	if isTxStart {
		length, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		db.TxStart = &length
	}

	result, err := part.readSlotPointer(db, isTopLevel, writeMode, path, pathI, slotPtr)

	if isTxStart {
		db.TxStart = nil
	}

	return result, err
}

// HashMap methods

func (db *Database) readMapSlot(indexPos int64, keyHash []byte, keyOffset byte, writeMode WriteMode, isTopLevel bool, target HashMapGetTarget) (HashMapGetResult, error) {
	if int(keyOffset) > (int(db.Header.HashSize)*8)/BitCount {
		return HashMapGetResult{}, ErrKeyOffsetExceeded
	}

	hashInt := new(big.Int).SetBytes(keyHash)
	i := int(new(big.Int).And(new(big.Int).Rsh(hashInt, uint(keyOffset)*BitCount), BigMask).Int64())
	slotPos := indexPos + int64(SlotLength*i)
	if err := db.Core.SeekTo(slotPos); err != nil {
		return HashMapGetResult{}, err
	}
	var slotBytes [SlotLength]byte
	if err := db.Core.Read(slotBytes[:]); err != nil {
		return HashMapGetResult{}, err
	}
	slot := SlotFromBytes(slotBytes)

	ptr := slot.Value

	switch slot.Tag {
	case TagNone:
		switch writeMode {
		case ReadOnly:
			return HashMapGetResult{}, ErrKeyNotFound
		case ReadWrite:
			// write hash and key/val slots
			hashPos, err := db.Core.Length()
			if err != nil {
				return HashMapGetResult{}, err
			}
			if err := db.Core.SeekTo(hashPos); err != nil {
				return HashMapGetResult{}, err
			}
			keySlotPos := hashPos + int64(db.Header.HashSize)
			valueSlotPos := keySlotPos + int64(SlotLength)
			kvPair := KeyValuePair{ValueSlot: Slot{}, KeySlot: Slot{}, Hash: keyHash}
			if err := db.Core.Write(kvPair.ToBytes()); err != nil {
				return HashMapGetResult{}, err
			}

			// point slot to hash pos
			nextSlot := Slot{Value: hashPos, Tag: TagKVPair}
			if err := db.Core.SeekTo(slotPos); err != nil {
				return HashMapGetResult{}, err
			}
			nextSlotBytes := nextSlot.ToBytes()
			if err := db.Core.Write(nextSlotBytes[:]); err != nil {
				return HashMapGetResult{}, err
			}

			var nextSlotPtr SlotPointer
			switch target.(type) {
			case HashMapGetKVPair:
				nextSlotPtr = SlotPointer{Position: &slotPos, Slot: nextSlot}
			case HashMapGetKey:
				nextSlotPtr = SlotPointer{Position: &keySlotPos, Slot: kvPair.KeySlot}
			case HashMapGetValue:
				nextSlotPtr = SlotPointer{Position: &valueSlotPos, Slot: kvPair.ValueSlot}
			}
			return HashMapGetResult{SlotPtr: nextSlotPtr, IsEmpty: true}, nil
		}

	case TagIndex:
		nextPtr := ptr
		if writeMode == ReadWrite && !isTopLevel {
			if db.TxStart != nil {
				if nextPtr < *db.TxStart {
					if err := db.Core.SeekTo(ptr); err != nil {
						return HashMapGetResult{}, err
					}
					indexBlock := make([]byte, IndexBlockSize)
					if err := db.Core.Read(indexBlock); err != nil {
						return HashMapGetResult{}, err
					}
					var err error
					nextPtr, err = db.Core.Length()
					if err != nil {
						return HashMapGetResult{}, err
					}
					if err := db.Core.SeekTo(nextPtr); err != nil {
						return HashMapGetResult{}, err
					}
					if err := db.Core.Write(indexBlock); err != nil {
						return HashMapGetResult{}, err
					}
					if err := db.Core.SeekTo(slotPos); err != nil {
						return HashMapGetResult{}, err
					}
					newSlot := Slot{Value: nextPtr, Tag: TagIndex}
					newSlotBytes := newSlot.ToBytes()
					if err := db.Core.Write(newSlotBytes[:]); err != nil {
						return HashMapGetResult{}, err
					}
				}
			} else if db.Header.Tag == TagArrayList {
				return HashMapGetResult{}, ErrExpectedTxStart
			}
		}
		return db.readMapSlot(nextPtr, keyHash, keyOffset+1, writeMode, isTopLevel, target)

	case TagKVPair:
		if err := db.Core.SeekTo(ptr); err != nil {
			return HashMapGetResult{}, err
		}
		kvPairBytes := make([]byte, KeyValuePairLength(int(db.Header.HashSize)))
		if err := db.Core.Read(kvPairBytes); err != nil {
			return HashMapGetResult{}, err
		}
		kvPair := KeyValuePairFromBytes(kvPairBytes, int(db.Header.HashSize))

		if bytes.Equal(kvPair.Hash, keyHash) {
			if writeMode == ReadWrite && !isTopLevel {
				if db.TxStart != nil {
					if ptr < *db.TxStart {
						hashPos, err := db.Core.Length()
						if err != nil {
							return HashMapGetResult{}, err
						}
						if err := db.Core.SeekTo(hashPos); err != nil {
							return HashMapGetResult{}, err
						}
						keySlotPos := hashPos + int64(db.Header.HashSize)
						valueSlotPos := keySlotPos + int64(SlotLength)
						if err := db.Core.Write(kvPair.ToBytes()); err != nil {
							return HashMapGetResult{}, err
						}

						nextSlot := Slot{Value: hashPos, Tag: TagKVPair}
						if err := db.Core.SeekTo(slotPos); err != nil {
							return HashMapGetResult{}, err
						}
						nextSlotBytes := nextSlot.ToBytes()
						if err := db.Core.Write(nextSlotBytes[:]); err != nil {
							return HashMapGetResult{}, err
						}

						var nextSlotPtr SlotPointer
						switch target.(type) {
						case HashMapGetKVPair:
							nextSlotPtr = SlotPointer{Position: &slotPos, Slot: nextSlot}
						case HashMapGetKey:
							nextSlotPtr = SlotPointer{Position: &keySlotPos, Slot: kvPair.KeySlot}
						case HashMapGetValue:
							nextSlotPtr = SlotPointer{Position: &valueSlotPos, Slot: kvPair.ValueSlot}
						}
						return HashMapGetResult{SlotPtr: nextSlotPtr, IsEmpty: false}, nil
					}
				} else if db.Header.Tag == TagArrayList {
					return HashMapGetResult{}, ErrExpectedTxStart
				}
			}

			keySlotPos := ptr + int64(db.Header.HashSize)
			valueSlotPos := keySlotPos + int64(SlotLength)
			var nextSlotPtr SlotPointer
			switch target.(type) {
			case HashMapGetKVPair:
				nextSlotPtr = SlotPointer{Position: &slotPos, Slot: slot}
			case HashMapGetKey:
				nextSlotPtr = SlotPointer{Position: &keySlotPos, Slot: kvPair.KeySlot}
			case HashMapGetValue:
				nextSlotPtr = SlotPointer{Position: &valueSlotPos, Slot: kvPair.ValueSlot}
			}
			return HashMapGetResult{SlotPtr: nextSlotPtr, IsEmpty: false}, nil
		}

		// hash collision - different keys
		switch writeMode {
		case ReadOnly:
			return HashMapGetResult{}, ErrKeyNotFound
		case ReadWrite:
			if int(keyOffset)+1 >= (int(db.Header.HashSize)*8)/BitCount {
				return HashMapGetResult{}, ErrKeyOffsetExceeded
			}
			existingHashInt := new(big.Int).SetBytes(kvPair.Hash)
			nextI := int(new(big.Int).And(new(big.Int).Rsh(existingHashInt, uint(keyOffset+1)*BitCount), BigMask).Int64())
			nextIndexPos, err := db.Core.Length()
			if err != nil {
				return HashMapGetResult{}, err
			}
			if err := db.Core.SeekTo(nextIndexPos); err != nil {
				return HashMapGetResult{}, err
			}
			if err := db.Core.Write(make([]byte, IndexBlockSize)); err != nil {
				return HashMapGetResult{}, err
			}
			if err := db.Core.SeekTo(nextIndexPos + int64(SlotLength*nextI)); err != nil {
				return HashMapGetResult{}, err
			}
			slotToWrite := slot.ToBytes()
			if err := db.Core.Write(slotToWrite[:]); err != nil {
				return HashMapGetResult{}, err
			}
			res, err := db.readMapSlot(nextIndexPos, keyHash, keyOffset+1, writeMode, isTopLevel, target)
			if err != nil {
				return HashMapGetResult{}, err
			}
			if err := db.Core.SeekTo(slotPos); err != nil {
				return HashMapGetResult{}, err
			}
			indexSlot := Slot{Value: nextIndexPos, Tag: TagIndex}
			indexSlotBytes := indexSlot.ToBytes()
			if err := db.Core.Write(indexSlotBytes[:]); err != nil {
				return HashMapGetResult{}, err
			}
			return res, nil
		}

	default:
		return HashMapGetResult{}, ErrUnexpectedTag
	}

	return HashMapGetResult{}, ErrUnreachable
}

func (db *Database) removeMapSlot(indexPos int64, keyHash []byte, keyOffset byte, isTopLevel bool) (Slot, error) {
	if int(keyOffset) > (int(db.Header.HashSize)*8)/BitCount {
		return Slot{}, ErrKeyOffsetExceeded
	}

	// read block
	var slotBlock [SlotCount]Slot
	if err := db.Core.SeekTo(indexPos); err != nil {
		return Slot{}, err
	}
	indexBlock := make([]byte, IndexBlockSize)
	if err := db.Core.Read(indexBlock); err != nil {
		return Slot{}, err
	}
	for j := 0; j < SlotCount; j++ {
		var sb [SlotLength]byte
		copy(sb[:], indexBlock[j*SlotLength:(j+1)*SlotLength])
		slotBlock[j] = SlotFromBytes(sb)
	}

	hashInt := new(big.Int).SetBytes(keyHash)
	i := int(new(big.Int).And(new(big.Int).Rsh(hashInt, uint(keyOffset)*BitCount), BigMask).Int64())
	slotPos := indexPos + int64(SlotLength*i)
	slot := slotBlock[i]

	var nextSlot Slot
	var err error

	switch slot.Tag {
	case TagNone:
		return Slot{}, ErrKeyNotFound
	case TagIndex:
		nextSlot, err = db.removeMapSlot(slot.Value, keyHash, keyOffset+1, isTopLevel)
		if err != nil {
			return Slot{}, err
		}
	case TagKVPair:
		if err := db.Core.SeekTo(slot.Value); err != nil {
			return Slot{}, err
		}
		kvPairBytes := make([]byte, KeyValuePairLength(int(db.Header.HashSize)))
		if err := db.Core.Read(kvPairBytes); err != nil {
			return Slot{}, err
		}
		kvPair := KeyValuePairFromBytes(kvPairBytes, int(db.Header.HashSize))
		if bytes.Equal(kvPair.Hash, keyHash) {
			nextSlot = Slot{}
		} else {
			return Slot{}, ErrKeyNotFound
		}
	default:
		return Slot{}, ErrUnexpectedTag
	}

	// if we're the root node, just write the new slot and finish
	if keyOffset == 0 {
		if err := db.Core.SeekTo(slotPos); err != nil {
			return Slot{}, err
		}
		nextSlotBytes := nextSlot.ToBytes()
		if err := db.Core.Write(nextSlotBytes[:]); err != nil {
			return Slot{}, err
		}
		return Slot{Value: indexPos, Tag: TagIndex}, nil
	}

	// get slot to return if there is only one used slot in this index block
	slotBlock[i] = nextSlot
	slotToReturn := (*Slot)(nil)
	emptySlot := Slot{}
	slotToReturn = &emptySlot
	for _, blockSlot := range slotBlock {
		if blockSlot.Tag == TagNone {
			continue
		}
		if slotToReturn != nil {
			if slotToReturn.Tag != TagNone {
				slotToReturn = nil
				break
			}
		}
		bs := blockSlot
		slotToReturn = &bs
	}

	if slotToReturn != nil {
		switch slotToReturn.Tag {
		case TagNone, TagKVPair:
			return *slotToReturn, nil
		}
	}

	// there was more than one used slot, or a single INDEX slot
	if !isTopLevel {
		if db.TxStart != nil {
			if indexPos < *db.TxStart {
				nextIndexPos, err := db.Core.Length()
				if err != nil {
					return Slot{}, err
				}
				if err := db.Core.SeekTo(nextIndexPos); err != nil {
					return Slot{}, err
				}
				if err := db.Core.Write(indexBlock); err != nil {
					return Slot{}, err
				}
				nextSlotPos := nextIndexPos + int64(SlotLength*i)
				if err := db.Core.SeekTo(nextSlotPos); err != nil {
					return Slot{}, err
				}
				nsb := nextSlot.ToBytes()
				if err := db.Core.Write(nsb[:]); err != nil {
					return Slot{}, err
				}
				return Slot{Value: nextIndexPos, Tag: TagIndex}, nil
			}
		} else if db.Header.Tag == TagArrayList {
			return Slot{}, ErrExpectedTxStart
		}
	}

	if err := db.Core.SeekTo(slotPos); err != nil {
		return Slot{}, err
	}
	nsb := nextSlot.ToBytes()
	if err := db.Core.Write(nsb[:]); err != nil {
		return Slot{}, err
	}
	return Slot{Value: indexPos, Tag: TagIndex}, nil
}

// ArrayList methods

func (db *Database) readArrayListSlotAppend(header ArrayListHeader, writeMode WriteMode, isTopLevel bool) (ArrayListAppendResult, error) {
	indexPos := header.Ptr
	key := header.Size

	var prevShift, nextShift byte
	if key < SlotCount {
		prevShift = 0
	} else {
		prevShift = byte(math.Log(float64(key-1)) / math.Log(float64(SlotCount)))
	}
	if key < SlotCount {
		nextShift = 0
	} else {
		nextShift = byte(math.Log(float64(key)) / math.Log(float64(SlotCount)))
	}

	if prevShift != nextShift {
		// root overflow
		nextIndexPos, err := db.Core.Length()
		if err != nil {
			return ArrayListAppendResult{}, err
		}
		if err := db.Core.SeekTo(nextIndexPos); err != nil {
			return ArrayListAppendResult{}, err
		}
		if err := db.Core.Write(make([]byte, IndexBlockSize)); err != nil {
			return ArrayListAppendResult{}, err
		}
		if err := db.Core.SeekTo(nextIndexPos); err != nil {
			return ArrayListAppendResult{}, err
		}
		indexSlot := Slot{Value: indexPos, Tag: TagIndex}
		isb := indexSlot.ToBytes()
		if err := db.Core.Write(isb[:]); err != nil {
			return ArrayListAppendResult{}, err
		}
		indexPos = nextIndexPos
	}

	slotPtr, err := db.readArrayListSlot(indexPos, key, nextShift, writeMode, isTopLevel)
	if err != nil {
		return ArrayListAppendResult{}, err
	}
	return ArrayListAppendResult{
		Header:  ArrayListHeader{Ptr: indexPos, Size: header.Size + 1},
		SlotPtr: slotPtr,
	}, nil
}

func (db *Database) readArrayListSlot(indexPos int64, key int64, shift byte, writeMode WriteMode, isTopLevel bool) (SlotPointer, error) {
	if shift >= MaxBranchLength {
		return SlotPointer{}, ErrMaxShiftExceeded
	}

	i := (key >> (int64(shift) * BitCount)) & Mask
	slotPos := indexPos + int64(SlotLength)*i
	if err := db.Core.SeekTo(slotPos); err != nil {
		return SlotPointer{}, err
	}
	var slotBytes [SlotLength]byte
	if err := db.Core.Read(slotBytes[:]); err != nil {
		return SlotPointer{}, err
	}
	slot := SlotFromBytes(slotBytes)

	if shift == 0 {
		return SlotPointer{Position: &slotPos, Slot: slot}, nil
	}

	ptr := slot.Value

	switch slot.Tag {
	case TagNone:
		switch writeMode {
		case ReadOnly:
			return SlotPointer{}, ErrKeyNotFound
		case ReadWrite:
			nextIndexPos, err := db.Core.Length()
			if err != nil {
				return SlotPointer{}, err
			}
			if err := db.Core.SeekTo(nextIndexPos); err != nil {
				return SlotPointer{}, err
			}
			if err := db.Core.Write(make([]byte, IndexBlockSize)); err != nil {
				return SlotPointer{}, err
			}
			if isTopLevel {
				fileSize, err := db.Core.Length()
				if err != nil {
					return SlotPointer{}, err
				}
				if err := db.Core.SeekTo(int64(DatabaseStart) + int64(ArrayListHeaderLength)); err != nil {
					return SlotPointer{}, err
				}
				if err := writeLong(db.Core, fileSize); err != nil {
					return SlotPointer{}, err
				}
			}
			if err := db.Core.SeekTo(slotPos); err != nil {
				return SlotPointer{}, err
			}
			newSlot := Slot{Value: nextIndexPos, Tag: TagIndex}
			nsb := newSlot.ToBytes()
			if err := db.Core.Write(nsb[:]); err != nil {
				return SlotPointer{}, err
			}
			return db.readArrayListSlot(nextIndexPos, key, shift-1, writeMode, isTopLevel)
		}
	case TagIndex:
		nextPtr := ptr
		if writeMode == ReadWrite && !isTopLevel {
			if db.TxStart != nil {
				if nextPtr < *db.TxStart {
					if err := db.Core.SeekTo(ptr); err != nil {
						return SlotPointer{}, err
					}
					indexBlock := make([]byte, IndexBlockSize)
					if err := db.Core.Read(indexBlock); err != nil {
						return SlotPointer{}, err
					}
					var err error
					nextPtr, err = db.Core.Length()
					if err != nil {
						return SlotPointer{}, err
					}
					if err := db.Core.SeekTo(nextPtr); err != nil {
						return SlotPointer{}, err
					}
					if err := db.Core.Write(indexBlock); err != nil {
						return SlotPointer{}, err
					}
					if err := db.Core.SeekTo(slotPos); err != nil {
						return SlotPointer{}, err
					}
					newSlot := Slot{Value: nextPtr, Tag: TagIndex}
					nsb := newSlot.ToBytes()
					if err := db.Core.Write(nsb[:]); err != nil {
						return SlotPointer{}, err
					}
				}
			} else if db.Header.Tag == TagArrayList {
				return SlotPointer{}, ErrExpectedTxStart
			}
		}
		return db.readArrayListSlot(nextPtr, key, shift-1, writeMode, isTopLevel)
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}

	return SlotPointer{}, ErrUnreachable
}

func (db *Database) readArrayListSlice(header ArrayListHeader, size int64) (ArrayListHeader, error) {
	if size > header.Size || size < 0 {
		return ArrayListHeader{}, ErrKeyNotFound
	}

	var prevShift, nextShift byte
	if header.Size < SlotCount+1 {
		prevShift = 0
	} else {
		prevShift = byte(math.Log(float64(header.Size-1)) / math.Log(float64(SlotCount)))
	}
	if size < SlotCount+1 {
		nextShift = 0
	} else {
		nextShift = byte(math.Log(float64(size-1)) / math.Log(float64(SlotCount)))
	}

	if prevShift == nextShift {
		return ArrayListHeader{Ptr: header.Ptr, Size: size}, nil
	}

	shift := prevShift
	indexPos := header.Ptr
	for shift > nextShift {
		if err := db.Core.SeekTo(indexPos); err != nil {
			return ArrayListHeader{}, err
		}
		var slotBytes [SlotLength]byte
		if err := db.Core.Read(slotBytes[:]); err != nil {
			return ArrayListHeader{}, err
		}
		slot := SlotFromBytes(slotBytes)
		shift--
		indexPos = slot.Value
	}
	return ArrayListHeader{Ptr: indexPos, Size: size}, nil
}

// LinkedArrayList methods

// linked_array_list (backed by a count-augmented B-tree)

func (db *Database) readBTreeNode(ptr int64) (BTreeNode, error) {
	if err := db.Core.SeekTo(ptr); err != nil {
		return BTreeNode{}, err
	}
	var headerBytes [BTreeNodeHeaderSize]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return BTreeNode{}, err
	}
	kindInt := headerBytes[0]
	if kindInt > byte(BTreeKindBranch) {
		return BTreeNode{}, ErrInvalidBTreeNodeKind
	}
	kind := BTreeNodeKind(kindInt)
	num := int(headerBytes[1])
	if num > BTreeSlotCount {
		return BTreeNode{}, ErrInvalidBTreeNode
	}
	node := BTreeNode{Kind: kind, Num: num}
	switch kind {
	case BTreeKindLeaf:
		body := make([]byte, SlotLength*BTreeSlotCount)
		if err := db.Core.Read(body); err != nil {
			return BTreeNode{}, err
		}
		for i := 0; i < BTreeSlotCount; i++ {
			var sb [SlotLength]byte
			copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
			node.Values[i] = SlotFromBytes(sb)
		}
	case BTreeKindBranch:
		body := make([]byte, (SlotLength+8)*BTreeSlotCount)
		if err := db.Core.Read(body); err != nil {
			return BTreeNode{}, err
		}
		for i := 0; i < BTreeSlotCount; i++ {
			var sb [SlotLength]byte
			copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
			node.Children[i] = SlotFromBytes(sb)
		}
		countsOffset := SlotLength * BTreeSlotCount
		for i := 0; i < BTreeSlotCount; i++ {
			node.Counts[i] = int64(binary.BigEndian.Uint64(body[countsOffset+i*8 : countsOffset+i*8+8]))
		}
	}
	return node, nil
}

// always writes the node as a block at ptr. b-tree mutations are persistent: every
// node on the path from the root is rewritten, while untouched subtrees are shared by
// pointer.
func (db *Database) writeBTreeNodeAt(node BTreeNode, ptr int64) error {
	if err := db.Core.SeekTo(ptr); err != nil {
		return err
	}
	var bodySize int
	if node.Kind == BTreeKindLeaf {
		bodySize = BTreeLeafBlockSize
	} else {
		bodySize = BTreeBranchBlockSize
	}
	buf := make([]byte, bodySize)
	buf[0] = byte(node.Kind)
	buf[1] = byte(node.Num)
	off := BTreeNodeHeaderSize
	switch node.Kind {
	case BTreeKindLeaf:
		for i := 0; i < BTreeSlotCount; i++ {
			sb := node.Values[i].ToBytes()
			copy(buf[off:], sb[:])
			off += SlotLength
		}
	case BTreeKindBranch:
		for i := 0; i < BTreeSlotCount; i++ {
			sb := node.Children[i].ToBytes()
			copy(buf[off:], sb[:])
			off += SlotLength
		}
		for i := 0; i < BTreeSlotCount; i++ {
			binary.BigEndian.PutUint64(buf[off:], uint64(node.Counts[i]))
			off += 8
		}
	}
	return db.Core.Write(buf)
}

// appends the node as a fresh block and returns its position
func (db *Database) writeBTreeNode(node BTreeNode) (int64, error) {
	ptr, err := db.Core.Length()
	if err != nil {
		return 0, err
	}
	if err := db.writeBTreeNodeAt(node, ptr); err != nil {
		return 0, err
	}
	return ptr, nil
}

// a node is safe to mutate in place when it was created in the current transaction
// (offset >= TxStart), since no committed moment and no pre-concat sharing can
// reference it. concat advances TxStart (an implicit freeze) precisely so its shared
// subtrees fall below it here. for an ephemeral (non-array-list) top level there is no
// transaction, so everything is mutable in place until a concat first sets TxStart.
func (db *Database) btreeReusable(ptr int64) bool {
	if db.TxStart != nil {
		return ptr >= *db.TxStart
	}
	return db.Header.Tag != TagArrayList
}

// write a new version of a node, reusing oldPtr's position in place if that node
// belongs to this transaction, otherwise appending a copy
func (db *Database) btreeWriteNode(node BTreeNode, oldPtr int64) (int64, error) {
	if db.btreeReusable(oldPtr) {
		if err := db.writeBTreeNodeAt(node, oldPtr); err != nil {
			return 0, err
		}
		return oldPtr, nil
	}
	return db.writeBTreeNode(node)
}

func (db *Database) btreeNewRoot() (int64, error) {
	return db.writeBTreeNode(BTreeNode{Kind: BTreeKindLeaf, Num: 0})
}

// descend to the value slot at the given rank (0-based), returning a pointer to it (its
// file position and current slot).
func (db *Database) readBTreeSlot(rootPtr, rank int64) (SlotPointer, error) {
	nodePtr := rootPtr
	rem := rank
	for {
		node, err := db.readBTreeNode(nodePtr)
		if err != nil {
			return SlotPointer{}, err
		}
		if node.Kind == BTreeKindLeaf {
			position := nodePtr + BTreeNodeHeaderSize + rem*SlotLength
			return SlotPointer{Position: &position, Slot: node.Values[rem]}, nil
		}
		i := 0
		for i+1 < node.Num && rem >= node.Counts[i] {
			rem -= node.Counts[i]
			i++
		}
		nodePtr = node.Children[i].Value
	}
}

// insert a placeholder slot at `rank` within the subtree at nodePtr, writing new nodes
// along the path. the caller fills in the value at the returned ValuePosition.
func (db *Database) btreeInsert(nodePtr, rank int64) (BTreeInsertResult, error) {
	node, err := db.readBTreeNode(nodePtr)
	if err != nil {
		return BTreeInsertResult{}, err
	}
	switch node.Kind {
	case BTreeKindLeaf:
		// build the entries with a placeholder spliced in at `rank`. the placeholder is
		// a NONE slot marked full so that, if the caller never writes a value (e.g.
		// appendCursor), iteration still counts it as an element rather than skipping it
		// as padding.
		r := int(rank)
		vals := make([]Slot, 0, BTreeSlotCount+1)
		vals = append(vals, node.Values[:r]...)
		vals = append(vals, Slot{Value: 0, Tag: TagNone, Full: true})
		vals = append(vals, node.Values[r:node.Num]...)
		total := node.Num + 1

		if total <= BTreeSlotCount {
			leaf := BTreeNode{Kind: BTreeKindLeaf, Num: total}
			copy(leaf.Values[:total], vals[:total])
			ptr, err := db.btreeWriteNode(leaf, nodePtr)
			if err != nil {
				return BTreeInsertResult{}, err
			}
			return BTreeInsertResult{NodePtr: ptr, Count: int64(total), ValuePosition: ptr + BTreeNodeHeaderSize + int64(r)*SlotLength}, nil
		}

		// overflow: split into two leaves (reuse this node for the left half)
		leftN := BTreeSplitCount
		rightN := total - leftN
		left := BTreeNode{Kind: BTreeKindLeaf, Num: leftN}
		copy(left.Values[:leftN], vals[:leftN])
		right := BTreeNode{Kind: BTreeKindLeaf, Num: rightN}
		copy(right.Values[:rightN], vals[leftN:total])
		leftPtr, err := db.btreeWriteNode(left, nodePtr)
		if err != nil {
			return BTreeInsertResult{}, err
		}
		rightPtr, err := db.writeBTreeNode(right)
		if err != nil {
			return BTreeInsertResult{}, err
		}
		var valuePosition int64
		if r < leftN {
			valuePosition = leftPtr + BTreeNodeHeaderSize + int64(r)*SlotLength
		} else {
			valuePosition = rightPtr + BTreeNodeHeaderSize + int64(r-leftN)*SlotLength
		}
		return BTreeInsertResult{NodePtr: leftPtr, Count: int64(leftN), ValuePosition: valuePosition, Split: &BTreeNodeRef{NodePtr: rightPtr, Count: int64(rightN)}}, nil
	case BTreeKindBranch:
		// pick the child that contains `rank`
		i := 0
		rem := rank
		for i+1 < node.Num && rem > node.Counts[i] {
			rem -= node.Counts[i]
			i++
		}
		child, err := db.btreeInsert(node.Children[i].Value, rem)
		if err != nil {
			return BTreeInsertResult{}, err
		}

		// rebuild this branch with the (possibly split) child
		children := make([]Slot, node.Num, BTreeSlotCount+1)
		counts := make([]int64, node.Num, BTreeSlotCount+1)
		copy(children, node.Children[:node.Num])
		copy(counts, node.Counts[:node.Num])
		children[i] = Slot{Value: child.NodePtr, Tag: TagIndex}
		counts[i] = child.Count
		total := node.Num
		if child.Split != nil {
			children = children[:node.Num+1]
			counts = counts[:node.Num+1]
			for j := node.Num; j > i+1; j-- {
				children[j] = children[j-1]
				counts[j] = counts[j-1]
			}
			children[i+1] = Slot{Value: child.Split.NodePtr, Tag: TagIndex}
			counts[i+1] = child.Split.Count
			total = node.Num + 1
		}

		if total <= BTreeSlotCount {
			branch := BTreeNode{Kind: BTreeKindBranch, Num: total}
			copy(branch.Children[:total], children[:total])
			copy(branch.Counts[:total], counts[:total])
			ptr, err := db.btreeWriteNode(branch, nodePtr)
			if err != nil {
				return BTreeInsertResult{}, err
			}
			return BTreeInsertResult{NodePtr: ptr, Count: branch.SubtreeCount(), ValuePosition: child.ValuePosition}, nil
		}

		// overflow: split into two branches (reuse this node for the left half)
		leftN := BTreeSplitCount
		rightN := total - leftN
		left := BTreeNode{Kind: BTreeKindBranch, Num: leftN}
		copy(left.Children[:leftN], children[:leftN])
		copy(left.Counts[:leftN], counts[:leftN])
		right := BTreeNode{Kind: BTreeKindBranch, Num: rightN}
		copy(right.Children[:rightN], children[leftN:total])
		copy(right.Counts[:rightN], counts[leftN:total])
		leftPtr, err := db.btreeWriteNode(left, nodePtr)
		if err != nil {
			return BTreeInsertResult{}, err
		}
		rightPtr, err := db.writeBTreeNode(right)
		if err != nil {
			return BTreeInsertResult{}, err
		}
		return BTreeInsertResult{NodePtr: leftPtr, Count: left.SubtreeCount(), ValuePosition: child.ValuePosition, Split: &BTreeNodeRef{NodePtr: rightPtr, Count: right.SubtreeCount()}}, nil
	}
	return BTreeInsertResult{}, ErrUnreachable
}

// turn an insert result into a root pointer, growing the tree a level if the old root
// split (shares the root-building logic with btreeMakeRoot)
func (db *Database) btreeGrowRoot(result BTreeInsertResult) (int64, error) {
	return db.btreeMakeRoot(BTreeJoinResult{NodePtr: result.NodePtr, Count: result.Count, Split: result.Split})
}

// descend to the value slot at `rank` for writing, copy-on-writing only the nodes that
// belong to a past transaction. the element count is unchanged, so when the whole path
// is already this-transaction nothing is rewritten and the caller writes straight into
// the existing leaf.
func (db *Database) btreeGetForWrite(nodePtr, rank int64) (BTreeWriteSlot, error) {
	node, err := db.readBTreeNode(nodePtr)
	if err != nil {
		return BTreeWriteSlot{}, err
	}
	if node.Kind == BTreeKindLeaf {
		newPtr := nodePtr
		if !db.btreeReusable(nodePtr) {
			newPtr, err = db.writeBTreeNode(node)
			if err != nil {
				return BTreeWriteSlot{}, err
			}
		}
		return BTreeWriteSlot{NodePtr: newPtr, ValuePosition: newPtr + BTreeNodeHeaderSize + rank*SlotLength, Slot: node.Values[rank]}, nil
	}
	i := 0
	rem := rank
	for i+1 < node.Num && rem >= node.Counts[i] {
		rem -= node.Counts[i]
		i++
	}
	childPtr := node.Children[i].Value
	child, err := db.btreeGetForWrite(childPtr, rem)
	if err != nil {
		return BTreeWriteSlot{}, err
	}
	// if the child stayed put, this branch is unchanged too
	if child.NodePtr == childPtr {
		return BTreeWriteSlot{NodePtr: nodePtr, ValuePosition: child.ValuePosition, Slot: child.Slot}, nil
	}
	node.Children[i] = Slot{Value: child.NodePtr, Tag: TagIndex}
	newPtr, err := db.btreeWriteNode(node, nodePtr)
	if err != nil {
		return BTreeWriteSlot{}, err
	}
	return BTreeWriteSlot{NodePtr: newPtr, ValuePosition: child.ValuePosition, Slot: child.Slot}, nil
}

// height of a tree = number of branch levels above the leaves
func (db *Database) btreeHeight(rootPtr int64) (int, error) {
	ptr := rootPtr
	height := 0
	for {
		node, err := db.readBTreeNode(ptr)
		if err != nil {
			return 0, err
		}
		if node.Kind == BTreeKindLeaf {
			return height, nil
		}
		height++
		ptr = node.Children[0].Value
	}
}

func (db *Database) btreeMakeRoot(result BTreeJoinResult) (int64, error) {
	if result.Split != nil {
		root := BTreeNode{Kind: BTreeKindBranch, Num: 2}
		root.Children[0] = Slot{Value: result.NodePtr, Tag: TagIndex}
		root.Children[1] = Slot{Value: result.Split.NodePtr, Tag: TagIndex}
		root.Counts[0] = result.Count
		root.Counts[1] = result.Split.Count
		return db.writeBTreeNode(root)
	}
	return result.NodePtr, nil
}

// write `vals` as one leaf, or split into two balanced leaves if it exceeds the node
// capacity
func (db *Database) btreeAssembleLeaf(vals []Slot, total int) (BTreeJoinResult, error) {
	if total <= BTreeSlotCount {
		leaf := BTreeNode{Kind: BTreeKindLeaf, Num: total}
		copy(leaf.Values[:total], vals[:total])
		ptr, err := db.writeBTreeNode(leaf)
		if err != nil {
			return BTreeJoinResult{}, err
		}
		return BTreeJoinResult{NodePtr: ptr, Count: int64(total)}, nil
	}
	leftN := total / 2
	left := BTreeNode{Kind: BTreeKindLeaf, Num: leftN}
	copy(left.Values[:leftN], vals[:leftN])
	right := BTreeNode{Kind: BTreeKindLeaf, Num: total - leftN}
	copy(right.Values[:total-leftN], vals[leftN:total])
	leftPtr, err := db.writeBTreeNode(left)
	if err != nil {
		return BTreeJoinResult{}, err
	}
	rightPtr, err := db.writeBTreeNode(right)
	if err != nil {
		return BTreeJoinResult{}, err
	}
	return BTreeJoinResult{NodePtr: leftPtr, Count: int64(leftN), Split: &BTreeNodeRef{NodePtr: rightPtr, Count: int64(total - leftN)}}, nil
}

// write `children`/`counts` as one branch, or split into two balanced branches
func (db *Database) btreeAssembleBranch(children []Slot, counts []int64, total int) (BTreeJoinResult, error) {
	if total <= BTreeSlotCount {
		branch := BTreeNode{Kind: BTreeKindBranch, Num: total}
		copy(branch.Children[:total], children[:total])
		copy(branch.Counts[:total], counts[:total])
		ptr, err := db.writeBTreeNode(branch)
		if err != nil {
			return BTreeJoinResult{}, err
		}
		return BTreeJoinResult{NodePtr: ptr, Count: branch.SubtreeCount()}, nil
	}
	leftN := total / 2
	left := BTreeNode{Kind: BTreeKindBranch, Num: leftN}
	copy(left.Children[:leftN], children[:leftN])
	copy(left.Counts[:leftN], counts[:leftN])
	right := BTreeNode{Kind: BTreeKindBranch, Num: total - leftN}
	copy(right.Children[:total-leftN], children[leftN:total])
	copy(right.Counts[:total-leftN], counts[leftN:total])
	leftPtr, err := db.writeBTreeNode(left)
	if err != nil {
		return BTreeJoinResult{}, err
	}
	rightPtr, err := db.writeBTreeNode(right)
	if err != nil {
		return BTreeJoinResult{}, err
	}
	return BTreeJoinResult{NodePtr: leftPtr, Count: left.SubtreeCount(), Split: &BTreeNodeRef{NodePtr: rightPtr, Count: right.SubtreeCount()}}, nil
}

// merge two nodes of equal height (a precedes b) into one or two nodes
func (db *Database) btreeMergeNodes(a, b BTreeNode) (BTreeJoinResult, error) {
	if a.Kind == BTreeKindLeaf {
		vals := make([]Slot, 0, 2*BTreeSlotCount)
		vals = append(vals, a.Values[:a.Num]...)
		vals = append(vals, b.Values[:b.Num]...)
		return db.btreeAssembleLeaf(vals, a.Num+b.Num)
	}
	children := make([]Slot, 0, 2*BTreeSlotCount)
	counts := make([]int64, 0, 2*BTreeSlotCount)
	children = append(children, a.Children[:a.Num]...)
	children = append(children, b.Children[:b.Num]...)
	counts = append(counts, a.Counts[:a.Num]...)
	counts = append(counts, b.Counts[:b.Num]...)
	return db.btreeAssembleBranch(children, counts, a.Num+b.Num)
}

// join b (shorter) into the rightmost spine of a (taller), at height hb
func (db *Database) btreeJoinRight(aPtr int64, ha int, bPtr int64, hb int) (BTreeJoinResult, error) {
	a, err := db.readBTreeNode(aPtr)
	if err != nil {
		return BTreeJoinResult{}, err
	}
	last := a.Num - 1
	var sub BTreeJoinResult
	if ha-1 == hb {
		lastChild, lerr := db.readBTreeNode(a.Children[last].Value)
		if lerr != nil {
			return BTreeJoinResult{}, lerr
		}
		bNode, berr := db.readBTreeNode(bPtr)
		if berr != nil {
			return BTreeJoinResult{}, berr
		}
		sub, err = db.btreeMergeNodes(lastChild, bNode)
	} else {
		sub, err = db.btreeJoinRight(a.Children[last].Value, ha-1, bPtr, hb)
	}
	if err != nil {
		return BTreeJoinResult{}, err
	}

	children := make([]Slot, a.Num, BTreeSlotCount+1)
	counts := make([]int64, a.Num, BTreeSlotCount+1)
	copy(children, a.Children[:a.Num])
	copy(counts, a.Counts[:a.Num])
	children[last] = Slot{Value: sub.NodePtr, Tag: TagIndex}
	counts[last] = sub.Count
	total := a.Num
	if sub.Split != nil {
		children = append(children, Slot{Value: sub.Split.NodePtr, Tag: TagIndex})
		counts = append(counts, sub.Split.Count)
		total++
	}
	return db.btreeAssembleBranch(children, counts, total)
}

// join a (shorter) into the leftmost spine of b (taller), at height ha
func (db *Database) btreeJoinLeft(aPtr int64, ha int, bPtr int64, hb int) (BTreeJoinResult, error) {
	b, err := db.readBTreeNode(bPtr)
	if err != nil {
		return BTreeJoinResult{}, err
	}
	var sub BTreeJoinResult
	if hb-1 == ha {
		aNode, aerr := db.readBTreeNode(aPtr)
		if aerr != nil {
			return BTreeJoinResult{}, aerr
		}
		firstChild, ferr := db.readBTreeNode(b.Children[0].Value)
		if ferr != nil {
			return BTreeJoinResult{}, ferr
		}
		sub, err = db.btreeMergeNodes(aNode, firstChild)
	} else {
		sub, err = db.btreeJoinLeft(aPtr, ha, b.Children[0].Value, hb-1)
	}
	if err != nil {
		return BTreeJoinResult{}, err
	}

	children := make([]Slot, 0, BTreeSlotCount+1)
	counts := make([]int64, 0, BTreeSlotCount+1)
	children = append(children, Slot{Value: sub.NodePtr, Tag: TagIndex})
	counts = append(counts, sub.Count)
	if sub.Split != nil {
		children = append(children, Slot{Value: sub.Split.NodePtr, Tag: TagIndex})
		counts = append(counts, sub.Split.Count)
	}
	children = append(children, b.Children[1:b.Num]...)
	counts = append(counts, b.Counts[1:b.Num]...)
	return db.btreeAssembleBranch(children, counts, len(children))
}

func (db *Database) btreeJoin(rootA, rootB int64) (int64, error) {
	ha, err := db.btreeHeight(rootA)
	if err != nil {
		return 0, err
	}
	hb, err := db.btreeHeight(rootB)
	if err != nil {
		return 0, err
	}
	var result BTreeJoinResult
	if ha == hb {
		a, aerr := db.readBTreeNode(rootA)
		if aerr != nil {
			return 0, aerr
		}
		b, berr := db.readBTreeNode(rootB)
		if berr != nil {
			return 0, berr
		}
		result, err = db.btreeMergeNodes(a, b)
	} else if ha > hb {
		result, err = db.btreeJoinRight(rootA, ha, rootB, hb)
	} else {
		result, err = db.btreeJoinLeft(rootA, ha, rootB, hb)
	}
	if err != nil {
		return 0, err
	}
	return db.btreeMakeRoot(result)
}

// build a tree from a run of sibling children (already height-h-1 subtrees): empty -> a
// new empty leaf, one -> that child unwrapped, many -> a branch
func (db *Database) btreeSubtree(children []Slot, counts []int64, start, length int) (int64, error) {
	if length == 0 {
		return db.btreeNewRoot()
	}
	if length == 1 {
		return children[start].Value, nil
	}
	subChildren := make([]Slot, length)
	subCounts := make([]int64, length)
	copy(subChildren, children[start:start+length])
	copy(subCounts, counts[start:start+length])
	res, err := db.btreeAssembleBranch(subChildren, subCounts, length)
	if err != nil {
		return 0, err
	}
	return res.NodePtr, nil
}

func (db *Database) btreeSplit(rootPtr, rank int64) (BTreeSplitResult, error) {
	node, err := db.readBTreeNode(rootPtr)
	if err != nil {
		return BTreeSplitResult{}, err
	}
	if node.Kind == BTreeKindLeaf {
		r := int(rank)
		left := BTreeNode{Kind: BTreeKindLeaf, Num: r}
		copy(left.Values[:r], node.Values[:r])
		right := BTreeNode{Kind: BTreeKindLeaf, Num: node.Num - r}
		copy(right.Values[:node.Num-r], node.Values[r:node.Num])
		leftPtr, lerr := db.writeBTreeNode(left)
		if lerr != nil {
			return BTreeSplitResult{}, lerr
		}
		rightPtr, rerr := db.writeBTreeNode(right)
		if rerr != nil {
			return BTreeSplitResult{}, rerr
		}
		return BTreeSplitResult{Left: leftPtr, Right: rightPtr}, nil
	}
	i := 0
	rem := rank
	for i+1 < node.Num && rem > node.Counts[i] {
		rem -= node.Counts[i]
		i++
	}
	child, err := db.btreeSplit(node.Children[i].Value, rem)
	if err != nil {
		return BTreeSplitResult{}, err
	}
	leftSub, err := db.btreeSubtree(node.Children[:], node.Counts[:], 0, i)
	if err != nil {
		return BTreeSplitResult{}, err
	}
	rightSub, err := db.btreeSubtree(node.Children[:], node.Counts[:], i+1, node.Num-(i+1))
	if err != nil {
		return BTreeSplitResult{}, err
	}
	joinedLeft, err := db.btreeJoin(leftSub, child.Left)
	if err != nil {
		return BTreeSplitResult{}, err
	}
	joinedRight, err := db.btreeJoin(child.Right, rightSub)
	if err != nil {
		return BTreeSplitResult{}, err
	}
	return BTreeSplitResult{Left: joinedLeft, Right: joinedRight}, nil
}

// Compaction helpers

func remapSlot(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, slot Slot) (Slot, error) {
	switch slot.Tag {
	case TagNone, TagUint, TagInt, TagFloat, TagShortBytes:
		return slot, nil
	case TagBytes:
		if mapped, ok := offsetMap[slot.Value]; ok {
			return Slot{Value: mapped, Tag: slot.Tag, Full: slot.Full}, nil
		}
		newOffset, err := remapBytes(sourceCore, targetCore, slot)
		if err != nil {
			return Slot{}, err
		}
		offsetMap[slot.Value] = newOffset
		return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
	case TagIndex:
		if mapped, ok := offsetMap[slot.Value]; ok {
			return Slot{Value: mapped, Tag: slot.Tag, Full: slot.Full}, nil
		}
		newOffset, err := remapIndex(sourceCore, targetCore, hashSize, offsetMap, slot)
		if err != nil {
			return Slot{}, err
		}
		offsetMap[slot.Value] = newOffset
		return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
	case TagArrayList:
		if mapped, ok := offsetMap[slot.Value]; ok {
			return Slot{Value: mapped, Tag: slot.Tag, Full: slot.Full}, nil
		}
		newOffset, err := remapArrayList(sourceCore, targetCore, hashSize, offsetMap, slot)
		if err != nil {
			return Slot{}, err
		}
		offsetMap[slot.Value] = newOffset
		return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
	case TagLinkedArrayList:
		if mapped, ok := offsetMap[slot.Value]; ok {
			return Slot{Value: mapped, Tag: slot.Tag, Full: slot.Full}, nil
		}
		newOffset, err := remapBTree(sourceCore, targetCore, hashSize, offsetMap, slot)
		if err != nil {
			return Slot{}, err
		}
		offsetMap[slot.Value] = newOffset
		return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
	case TagHashMap, TagHashSet:
		if mapped, ok := offsetMap[slot.Value]; ok {
			return Slot{Value: mapped, Tag: slot.Tag, Full: slot.Full}, nil
		}
		newOffset, err := remapHashMapOrSet(sourceCore, targetCore, hashSize, offsetMap, slot, false)
		if err != nil {
			return Slot{}, err
		}
		offsetMap[slot.Value] = newOffset
		return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
	case TagCountedHashMap, TagCountedHashSet:
		if mapped, ok := offsetMap[slot.Value]; ok {
			return Slot{Value: mapped, Tag: slot.Tag, Full: slot.Full}, nil
		}
		newOffset, err := remapHashMapOrSet(sourceCore, targetCore, hashSize, offsetMap, slot, true)
		if err != nil {
			return Slot{}, err
		}
		offsetMap[slot.Value] = newOffset
		return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
	case TagKVPair:
		if mapped, ok := offsetMap[slot.Value]; ok {
			return Slot{Value: mapped, Tag: slot.Tag, Full: slot.Full}, nil
		}
		newOffset, err := remapKvPair(sourceCore, targetCore, hashSize, offsetMap, slot)
		if err != nil {
			return Slot{}, err
		}
		offsetMap[slot.Value] = newOffset
		return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
	default:
		return Slot{}, ErrUnexpectedTag
	}
}

func remapBytes(sourceCore, targetCore Core, slot Slot) (int64, error) {
	if err := sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}
	length, err := readLong(sourceCore)
	if err != nil {
		return 0, err
	}

	formatTagSize := int64(0)
	if slot.Full {
		formatTagSize = 2
	}
	totalPayload := length + formatTagSize

	newOffset, err := targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := targetCore.SeekTo(newOffset); err != nil {
		return 0, err
	}
	if err := writeLong(targetCore, length); err != nil {
		return 0, err
	}

	remaining := totalPayload
	for remaining > 0 {
		chunk := int64(4096)
		if remaining < chunk {
			chunk = remaining
		}
		buf := make([]byte, chunk)
		if err := sourceCore.Read(buf); err != nil {
			return 0, err
		}
		if err := targetCore.Write(buf); err != nil {
			return 0, err
		}
		remaining -= chunk
	}

	return newOffset, nil
}

func remapIndex(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, slot Slot) (int64, error) {
	if err := sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}
	blockBytes := make([]byte, IndexBlockSize)
	if err := sourceCore.Read(blockBytes); err != nil {
		return 0, err
	}

	var remappedSlots [SlotCount]Slot
	for i := 0; i < SlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], blockBytes[i*SlotLength:(i+1)*SlotLength])
		childSlot := SlotFromBytes(sb)
		remapped, err := remapSlot(sourceCore, targetCore, hashSize, offsetMap, childSlot)
		if err != nil {
			return 0, err
		}
		remappedSlots[i] = remapped
	}

	newOffset, err := targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := targetCore.SeekTo(newOffset); err != nil {
		return 0, err
	}
	for _, s := range remappedSlots {
		b := s.ToBytes()
		if err := targetCore.Write(b[:]); err != nil {
			return 0, err
		}
	}

	return newOffset, nil
}

func remapArrayList(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, slot Slot) (int64, error) {
	if err := sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}
	var headerBytes [ArrayListHeaderLength]byte
	if err := sourceCore.Read(headerBytes[:]); err != nil {
		return 0, err
	}
	header, err := ArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return 0, err
	}

	indexSlot := Slot{Value: header.Ptr, Tag: TagIndex}
	remappedIndex, err := remapSlot(sourceCore, targetCore, hashSize, offsetMap, indexSlot)
	if err != nil {
		return 0, err
	}

	newOffset, err := targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := targetCore.SeekTo(newOffset); err != nil {
		return 0, err
	}
	newHeader := ArrayListHeader{Ptr: remappedIndex.Value, Size: header.Size}
	nhb := newHeader.ToBytes()
	if err := targetCore.Write(nhb[:]); err != nil {
		return 0, err
	}

	return newOffset, nil
}

func remapBTree(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, slot Slot) (int64, error) {
	if err := sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := sourceCore.Read(headerBytes[:]); err != nil {
		return 0, err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return 0, err
	}

	remappedRoot, err := remapBTreeNode(sourceCore, targetCore, hashSize, offsetMap, header.RootPtr)
	if err != nil {
		return 0, err
	}

	newOffset, err := targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := targetCore.SeekTo(newOffset); err != nil {
		return 0, err
	}
	newHeader := BTreeHeader{RootPtr: remappedRoot, Size: header.Size}
	nhb := newHeader.ToBytes()
	if err := targetCore.Write(nhb[:]); err != nil {
		return 0, err
	}

	return newOffset, nil
}

func remapBTreeNode(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, nodeOffset int64) (int64, error) {
	if mapped, ok := offsetMap[nodeOffset]; ok {
		return mapped, nil
	}

	if err := sourceCore.SeekTo(nodeOffset); err != nil {
		return 0, err
	}
	var nodeHeader [BTreeNodeHeaderSize]byte
	if err := sourceCore.Read(nodeHeader[:]); err != nil {
		return 0, err
	}
	kindInt := nodeHeader[0]
	if kindInt > byte(BTreeKindBranch) {
		return 0, ErrInvalidBTreeNodeKind
	}
	kind := BTreeNodeKind(kindInt)
	num := nodeHeader[1]

	switch kind {
	case BTreeKindLeaf:
		body := make([]byte, SlotLength*BTreeSlotCount)
		if err := sourceCore.Read(body); err != nil {
			return 0, err
		}
		var slots [BTreeSlotCount]Slot
		for i := 0; i < BTreeSlotCount; i++ {
			var sb [SlotLength]byte
			copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
			remapped, err := remapSlot(sourceCore, targetCore, hashSize, offsetMap, SlotFromBytes(sb))
			if err != nil {
				return 0, err
			}
			slots[i] = remapped
		}

		newOffset, err := targetCore.Length()
		if err != nil {
			return 0, err
		}
		if err := targetCore.SeekTo(newOffset); err != nil {
			return 0, err
		}
		if err := targetCore.Write([]byte{kindInt, num}); err != nil {
			return 0, err
		}
		for _, s := range slots {
			b := s.ToBytes()
			if err := targetCore.Write(b[:]); err != nil {
				return 0, err
			}
		}

		offsetMap[nodeOffset] = newOffset
		return newOffset, nil
	case BTreeKindBranch:
		body := make([]byte, (SlotLength+8)*BTreeSlotCount)
		if err := sourceCore.Read(body); err != nil {
			return 0, err
		}
		var children [BTreeSlotCount]Slot
		for i := 0; i < BTreeSlotCount; i++ {
			var sb [SlotLength]byte
			copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
			child := SlotFromBytes(sb)
			if child.Tag == TagIndex {
				remappedPtr, err := remapBTreeNode(sourceCore, targetCore, hashSize, offsetMap, child.Value)
				if err != nil {
					return 0, err
				}
				children[i] = Slot{Value: remappedPtr, Tag: TagIndex, Full: child.Full}
			} else {
				children[i] = child
			}
		}
		countsOffset := SlotLength * BTreeSlotCount
		var counts [BTreeSlotCount]int64
		for i := 0; i < BTreeSlotCount; i++ {
			counts[i] = int64(binary.BigEndian.Uint64(body[countsOffset+i*8 : countsOffset+i*8+8]))
		}

		newOffset, err := targetCore.Length()
		if err != nil {
			return 0, err
		}
		if err := targetCore.SeekTo(newOffset); err != nil {
			return 0, err
		}
		if err := targetCore.Write([]byte{kindInt, num}); err != nil {
			return 0, err
		}
		for _, s := range children {
			b := s.ToBytes()
			if err := targetCore.Write(b[:]); err != nil {
				return 0, err
			}
		}
		for _, c := range counts {
			var cb [8]byte
			binary.BigEndian.PutUint64(cb[:], uint64(c))
			if err := targetCore.Write(cb[:]); err != nil {
				return 0, err
			}
		}

		offsetMap[nodeOffset] = newOffset
		return newOffset, nil
	}
	return 0, ErrUnreachable
}

func remapHashMapOrSet(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, slot Slot, counted bool) (int64, error) {
	if err := sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}

	var countValue int64 = -1
	if counted {
		var err error
		countValue, err = readLong(sourceCore)
		if err != nil {
			return 0, err
		}
	}

	blockBytes := make([]byte, IndexBlockSize)
	if err := sourceCore.Read(blockBytes); err != nil {
		return 0, err
	}

	var remappedSlots [SlotCount]Slot
	for i := 0; i < SlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], blockBytes[i*SlotLength:(i+1)*SlotLength])
		childSlot := SlotFromBytes(sb)
		remapped, err := remapSlot(sourceCore, targetCore, hashSize, offsetMap, childSlot)
		if err != nil {
			return 0, err
		}
		remappedSlots[i] = remapped
	}

	newOffset, err := targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := targetCore.SeekTo(newOffset); err != nil {
		return 0, err
	}
	if counted {
		if err := writeLong(targetCore, countValue); err != nil {
			return 0, err
		}
	}
	for _, s := range remappedSlots {
		b := s.ToBytes()
		if err := targetCore.Write(b[:]); err != nil {
			return 0, err
		}
	}

	return newOffset, nil
}

func remapKvPair(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, slot Slot) (int64, error) {
	if err := sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}
	kvPairBytes := make([]byte, KeyValuePairLength(int(hashSize)))
	if err := sourceCore.Read(kvPairBytes); err != nil {
		return 0, err
	}
	kvPair := KeyValuePairFromBytes(kvPairBytes, int(hashSize))

	remappedKey, err := remapSlot(sourceCore, targetCore, hashSize, offsetMap, kvPair.KeySlot)
	if err != nil {
		return 0, err
	}
	remappedValue, err := remapSlot(sourceCore, targetCore, hashSize, offsetMap, kvPair.ValueSlot)
	if err != nil {
		return 0, err
	}

	newOffset, err := targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := targetCore.SeekTo(newOffset); err != nil {
		return 0, err
	}
	newKvPair := KeyValuePair{ValueSlot: remappedValue, KeySlot: remappedKey, Hash: kvPair.Hash}
	if err := targetCore.Write(newKvPair.ToBytes()); err != nil {
		return 0, err
	}

	return newOffset, nil
}
