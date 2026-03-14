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
	LinkedArrayListIndexBlockSize        = LinkedArrayListSlotLength * SlotCount
	MaxBranchLength                      = 16
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

// LinkedArrayListHeader

const LinkedArrayListHeaderLength = 17

type LinkedArrayListHeader struct {
	Shift byte
	Ptr   int64
	Size  int64
}

func (h LinkedArrayListHeader) ToBytes() [LinkedArrayListHeaderLength]byte {
	var buf [LinkedArrayListHeaderLength]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(h.Size))
	binary.BigEndian.PutUint64(buf[8:16], uint64(h.Ptr))
	buf[16] = h.Shift & 0b0011_1111
	return buf
}

func LinkedArrayListHeaderFromBytes(b []byte) (LinkedArrayListHeader, error) {
	size := int64(binary.BigEndian.Uint64(b[0:8]))
	ptr := int64(binary.BigEndian.Uint64(b[8:16]))
	shift := b[16] & 0b0011_1111
	if size < 0 {
		return LinkedArrayListHeader{}, ErrExpectedUnsignedLong
	}
	if ptr < 0 {
		return LinkedArrayListHeader{}, ErrExpectedUnsignedLong
	}
	return LinkedArrayListHeader{Shift: shift, Ptr: ptr, Size: size}, nil
}

func (h LinkedArrayListHeader) WithPtr(ptr int64) LinkedArrayListHeader {
	return LinkedArrayListHeader{Shift: h.Shift, Ptr: ptr, Size: h.Size}
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

// LinkedArrayListSlot

const LinkedArrayListSlotLength = 8 + SlotLength

type LinkedArrayListSlot struct {
	Size int64
	Slot Slot
}

func (s LinkedArrayListSlot) WithSize(size int64) LinkedArrayListSlot {
	return LinkedArrayListSlot{Size: size, Slot: s.Slot}
}

func (s LinkedArrayListSlot) ToBytes() [LinkedArrayListSlotLength]byte {
	var buf [LinkedArrayListSlotLength]byte
	slotBytes := s.Slot.ToBytes()
	copy(buf[0:SlotLength], slotBytes[:])
	binary.BigEndian.PutUint64(buf[SlotLength:], uint64(s.Size))
	return buf
}

func LinkedArrayListSlotFromBytes(b []byte) (LinkedArrayListSlot, error) {
	var slotBytes [SlotLength]byte
	copy(slotBytes[:], b[0:SlotLength])
	slot := SlotFromBytes(slotBytes)
	size := int64(binary.BigEndian.Uint64(b[SlotLength:]))
	if size < 0 {
		return LinkedArrayListSlot{}, ErrExpectedUnsignedLong
	}
	return LinkedArrayListSlot{Size: size, Slot: slot}, nil
}

// LinkedArrayListSlotPointer

type LinkedArrayListSlotPointer struct {
	SlotPtr   SlotPointer
	LeafCount int64
}

func (p LinkedArrayListSlotPointer) WithSlotPointer(sp SlotPointer) LinkedArrayListSlotPointer {
	return LinkedArrayListSlotPointer{SlotPtr: sp, LeafCount: p.LeafCount}
}

// LinkedArrayListBlockInfo

type LinkedArrayListBlockInfo struct {
	Block      [SlotCount]LinkedArrayListSlot
	I          byte
	ParentSlot LinkedArrayListSlot
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

// LinkedArrayListAppendResult

type LinkedArrayListAppendResult struct {
	Header  LinkedArrayListHeader
	SlotPtr LinkedArrayListSlotPointer
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

func (db *Database) Compact(targetCore Core, hasher Hasher) (*Database, error) {
	offsetMap := make(map[int64]int64)
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

func (db *Database) readLinkedArrayListSlotAppend(header LinkedArrayListHeader, writeMode WriteMode, isTopLevel bool) (LinkedArrayListAppendResult, error) {
	ptr := header.Ptr
	key := header.Size
	shift := header.Shift

	slotPtr, err := db.readLinkedArrayListSlot(ptr, key, shift, writeMode, isTopLevel)
	if err != nil {
		if err == ErrNoAvailableSlots {
			// root overflow
			nextPtr, err := db.Core.Length()
			if err != nil {
				return LinkedArrayListAppendResult{}, err
			}
			if err := db.Core.SeekTo(nextPtr); err != nil {
				return LinkedArrayListAppendResult{}, err
			}
			if err := db.Core.Write(make([]byte, LinkedArrayListIndexBlockSize)); err != nil {
				return LinkedArrayListAppendResult{}, err
			}
			if err := db.Core.SeekTo(nextPtr); err != nil {
				return LinkedArrayListAppendResult{}, err
			}
			laSlot := LinkedArrayListSlot{Size: header.Size, Slot: Slot{Value: ptr, Tag: TagIndex, Full: true}}
			lab := laSlot.ToBytes()
			if err := db.Core.Write(lab[:]); err != nil {
				return LinkedArrayListAppendResult{}, err
			}
			ptr = nextPtr
			shift++
			slotPtr, err = db.readLinkedArrayListSlot(ptr, key, shift, writeMode, isTopLevel)
			if err != nil {
				return LinkedArrayListAppendResult{}, err
			}
		} else {
			return LinkedArrayListAppendResult{}, err
		}
	}

	// newly-appended slots must have full set to true
	newSlot := Slot{Value: 0, Tag: TagNone, Full: true}
	slotPtr = slotPtr.WithSlotPointer(slotPtr.SlotPtr.WithSlot(newSlot))
	if slotPtr.SlotPtr.Position == nil {
		return LinkedArrayListAppendResult{}, ErrCursorNotWriteable
	}
	position := *slotPtr.SlotPtr.Position
	if err := db.Core.SeekTo(position); err != nil {
		return LinkedArrayListAppendResult{}, err
	}
	laSlot := LinkedArrayListSlot{Size: 0, Slot: newSlot}
	lab := laSlot.ToBytes()
	if err := db.Core.Write(lab[:]); err != nil {
		return LinkedArrayListAppendResult{}, err
	}
	if header.Size < SlotCount && shift > 0 {
		return LinkedArrayListAppendResult{}, ErrMustSetNewSlotsToFull
	}

	return LinkedArrayListAppendResult{
		Header:  LinkedArrayListHeader{Shift: shift, Ptr: ptr, Size: header.Size + 1},
		SlotPtr: slotPtr,
	}, nil
}

func blockLeafCount(block [SlotCount]LinkedArrayListSlot, shift byte, i byte) int64 {
	var n int64
	if shift == 0 {
		for blockI := 0; blockI < SlotCount; blockI++ {
			if !block[blockI].Slot.Empty() || byte(blockI) == i {
				n++
			}
		}
	} else {
		for _, blockSlot := range block {
			n += blockSlot.Size
		}
	}
	return n
}

func slotLeafCount(slot LinkedArrayListSlot, shift byte) int64 {
	if shift == 0 {
		if slot.Slot.Empty() {
			return 0
		}
		return 1
	}
	return slot.Size
}

type keyAndIndex struct {
	key   int64
	index byte
}

func keyAndIndexForLinkedArrayList(slotBlock [SlotCount]LinkedArrayListSlot, key int64, shift byte) *keyAndIndex {
	nextKey := key
	var i byte = 0
	var maxLeafCount int64
	if shift == 0 {
		maxLeafCount = 1
	} else {
		maxLeafCount = int64(math.Pow(float64(SlotCount), float64(shift)))
	}

	for {
		slc := slotLeafCount(slotBlock[i], shift)
		if nextKey == slc {
			if slc == maxLeafCount || slotBlock[i].Slot.Full {
				if i < SlotCount-1 {
					nextKey -= slc
					i++
				} else {
					return nil
				}
			}
			break
		} else if nextKey < slc {
			break
		} else if i < SlotCount-1 {
			nextKey -= slc
			i++
		} else {
			return nil
		}
	}
	return &keyAndIndex{key: nextKey, index: i}
}

func (db *Database) readLinkedArrayListSlot(indexPos int64, key int64, shift byte, writeMode WriteMode, isTopLevel bool) (LinkedArrayListSlotPointer, error) {
	if shift >= MaxBranchLength {
		return LinkedArrayListSlotPointer{}, ErrMaxShiftExceeded
	}

	var slotBlock [SlotCount]LinkedArrayListSlot
	if err := db.Core.SeekTo(indexPos); err != nil {
		return LinkedArrayListSlotPointer{}, err
	}
	indexBlockBytes := make([]byte, LinkedArrayListIndexBlockSize)
	if err := db.Core.Read(indexBlockBytes); err != nil {
		return LinkedArrayListSlotPointer{}, err
	}
	for j := 0; j < SlotCount; j++ {
		s, err := LinkedArrayListSlotFromBytes(indexBlockBytes[j*LinkedArrayListSlotLength : (j+1)*LinkedArrayListSlotLength])
		if err != nil {
			return LinkedArrayListSlotPointer{}, err
		}
		slotBlock[j] = s
	}

	ki := keyAndIndexForLinkedArrayList(slotBlock, key, shift)
	if ki == nil {
		return LinkedArrayListSlotPointer{}, ErrNoAvailableSlots
	}
	nextKey := ki.key
	i := ki.index
	slot := slotBlock[i]
	slotPos := indexPos + int64(LinkedArrayListSlotLength)*int64(i)

	if shift == 0 {
		leafCount := blockLeafCount(slotBlock, shift, i)
		return LinkedArrayListSlotPointer{
			SlotPtr:   SlotPointer{Position: &slotPos, Slot: slot.Slot},
			LeafCount: leafCount,
		}, nil
	}

	ptr := slot.Slot.Value

	switch slot.Slot.Tag {
	case TagNone:
		switch writeMode {
		case ReadOnly:
			return LinkedArrayListSlotPointer{}, ErrKeyNotFound
		case ReadWrite:
			nextIndexPos, err := db.Core.Length()
			if err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
			if err := db.Core.SeekTo(nextIndexPos); err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
			if err := db.Core.Write(make([]byte, LinkedArrayListIndexBlockSize)); err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
			nextSlotPtr, err := db.readLinkedArrayListSlot(nextIndexPos, nextKey, shift-1, writeMode, isTopLevel)
			if err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
			slotBlock[i] = slotBlock[i].WithSize(nextSlotPtr.LeafCount)
			leafCount := blockLeafCount(slotBlock, shift, i)
			if err := db.Core.SeekTo(slotPos); err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
			writeSlot := LinkedArrayListSlot{Size: nextSlotPtr.LeafCount, Slot: Slot{Value: nextIndexPos, Tag: TagIndex}}
			wsb := writeSlot.ToBytes()
			if err := db.Core.Write(wsb[:]); err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
			return LinkedArrayListSlotPointer{SlotPtr: nextSlotPtr.SlotPtr, LeafCount: leafCount}, nil
		}
	case TagIndex:
		nextPtr := ptr
		if writeMode == ReadWrite && !isTopLevel {
			if db.TxStart != nil {
				if nextPtr < *db.TxStart {
					if err := db.Core.SeekTo(ptr); err != nil {
						return LinkedArrayListSlotPointer{}, err
					}
					indexBlock := make([]byte, LinkedArrayListIndexBlockSize)
					if err := db.Core.Read(indexBlock); err != nil {
						return LinkedArrayListSlotPointer{}, err
					}
					var err error
					nextPtr, err = db.Core.Length()
					if err != nil {
						return LinkedArrayListSlotPointer{}, err
					}
					if err := db.Core.SeekTo(nextPtr); err != nil {
						return LinkedArrayListSlotPointer{}, err
					}
					if err := db.Core.Write(indexBlock); err != nil {
						return LinkedArrayListSlotPointer{}, err
					}
				}
			} else if db.Header.Tag == TagArrayList {
				return LinkedArrayListSlotPointer{}, ErrExpectedTxStart
			}
		}

		nextSlotPtr, err := db.readLinkedArrayListSlot(nextPtr, nextKey, shift-1, writeMode, isTopLevel)
		if err != nil {
			return LinkedArrayListSlotPointer{}, err
		}

		slotBlock[i] = slotBlock[i].WithSize(nextSlotPtr.LeafCount)
		leafCount := blockLeafCount(slotBlock, shift, i)

		if writeMode == ReadWrite && !isTopLevel {
			if err := db.Core.SeekTo(slotPos); err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
			writeSlot := LinkedArrayListSlot{Size: nextSlotPtr.LeafCount, Slot: Slot{Value: nextPtr, Tag: TagIndex}}
			wsb := writeSlot.ToBytes()
			if err := db.Core.Write(wsb[:]); err != nil {
				return LinkedArrayListSlotPointer{}, err
			}
		}

		return LinkedArrayListSlotPointer{SlotPtr: nextSlotPtr.SlotPtr, LeafCount: leafCount}, nil
	default:
		return LinkedArrayListSlotPointer{}, ErrUnexpectedTag
	}

	return LinkedArrayListSlotPointer{}, ErrUnreachable
}

func (db *Database) readLinkedArrayListBlocks(indexPos int64, key int64, shift byte, blocks *[]LinkedArrayListBlockInfo) error {
	var slotBlock [SlotCount]LinkedArrayListSlot
	if err := db.Core.SeekTo(indexPos); err != nil {
		return err
	}
	indexBlockBytes := make([]byte, LinkedArrayListIndexBlockSize)
	if err := db.Core.Read(indexBlockBytes); err != nil {
		return err
	}
	for j := 0; j < SlotCount; j++ {
		s, err := LinkedArrayListSlotFromBytes(indexBlockBytes[j*LinkedArrayListSlotLength : (j+1)*LinkedArrayListSlotLength])
		if err != nil {
			return err
		}
		slotBlock[j] = s
	}

	ki := keyAndIndexForLinkedArrayList(slotBlock, key, shift)
	if ki == nil {
		return ErrNoAvailableSlots
	}
	nextKey := ki.key
	i := ki.index
	leafCount := blockLeafCount(slotBlock, shift, i)

	*blocks = append(*blocks, LinkedArrayListBlockInfo{
		Block:      slotBlock,
		I:          i,
		ParentSlot: LinkedArrayListSlot{Size: leafCount, Slot: Slot{Value: indexPos, Tag: TagIndex}},
	})

	if shift == 0 {
		return nil
	}

	slot := slotBlock[i]
	switch slot.Slot.Tag {
	case TagNone:
		return ErrEmptySlot
	case TagIndex:
		return db.readLinkedArrayListBlocks(slot.Slot.Value, nextKey, shift-1, blocks)
	default:
		return ErrUnexpectedTag
	}
}

func populateArray(arr *[SlotCount]LinkedArrayListSlot) {
	for i := range arr {
		arr[i] = LinkedArrayListSlot{Size: 0, Slot: Slot{}}
	}
}

func (db *Database) readLinkedArrayListSlice(header LinkedArrayListHeader, offset int64, size int64) (LinkedArrayListHeader, error) {
	if offset+size > header.Size {
		return LinkedArrayListHeader{}, ErrKeyNotFound
	}

	// read the list's left blocks
	var leftBlocks []LinkedArrayListBlockInfo
	if err := db.readLinkedArrayListBlocks(header.Ptr, offset, header.Shift, &leftBlocks); err != nil {
		return LinkedArrayListHeader{}, err
	}

	// read the list's right blocks
	var rightBlocks []LinkedArrayListBlockInfo
	var rightKey int64
	if offset+size == 0 {
		rightKey = 0
	} else {
		rightKey = offset + size - 1
	}
	if err := db.readLinkedArrayListBlocks(header.Ptr, rightKey, header.Shift, &rightBlocks); err != nil {
		return LinkedArrayListHeader{}, err
	}

	blockCount := len(leftBlocks)
	nextSlots := [2]*LinkedArrayListSlot{nil, nil}
	var nextShift byte = 0

	for i := 0; i < blockCount; i++ {
		isLeafNode := nextSlots[0] == nil

		leftBlock := leftBlocks[blockCount-i-1]
		rightBlock := rightBlocks[blockCount-i-1]
		origBlockInfos := [2]LinkedArrayListBlockInfo{leftBlock, rightBlock}
		var nextBlocks [2]*[SlotCount]LinkedArrayListSlot

		if leftBlock.ParentSlot.Slot.Value == rightBlock.ParentSlot.Slot.Value {
			var slotI int
			var newRootBlock [SlotCount]LinkedArrayListSlot
			populateArray(&newRootBlock)
			if size > 0 {
				if nextSlots[0] != nil {
					newRootBlock[slotI] = *nextSlots[0]
				} else {
					newRootBlock[slotI] = leftBlock.Block[leftBlock.I]
				}
				slotI++
			}
			if size > 1 {
				for j := int(leftBlock.I) + 1; j < int(rightBlock.I); j++ {
					newRootBlock[slotI] = leftBlock.Block[j]
					slotI++
				}
				if nextSlots[1] != nil {
					newRootBlock[slotI] = *nextSlots[1]
				} else {
					newRootBlock[slotI] = leftBlock.Block[rightBlock.I]
				}
			}
			nextBlocks[0] = &newRootBlock
		} else {
			var slotI int
			var newLeftBlock [SlotCount]LinkedArrayListSlot
			populateArray(&newLeftBlock)
			if nextSlots[0] != nil {
				newLeftBlock[slotI] = *nextSlots[0]
			} else {
				newLeftBlock[slotI] = leftBlock.Block[leftBlock.I]
			}
			slotI++
			for j := int(leftBlock.I) + 1; j < SlotCount; j++ {
				newLeftBlock[slotI] = leftBlock.Block[j]
				slotI++
			}
			nextBlocks[0] = &newLeftBlock

			slotI = 0
			var newRightBlock [SlotCount]LinkedArrayListSlot
			populateArray(&newRightBlock)
			for j := 0; j < int(rightBlock.I); j++ {
				newRightBlock[slotI] = rightBlock.Block[j]
				slotI++
			}
			if nextSlots[1] != nil {
				newRightBlock[slotI] = *nextSlots[1]
			} else {
				newRightBlock[slotI] = rightBlock.Block[rightBlock.I]
			}
			nextBlocks[1] = &newRightBlock

			nextShift++
		}

		nextSlots = [2]*LinkedArrayListSlot{nil, nil}

		coreLen, err := db.Core.Length()
		if err != nil {
			return LinkedArrayListHeader{}, err
		}
		if err := db.Core.SeekTo(coreLen); err != nil {
			return LinkedArrayListHeader{}, err
		}

		for j := 0; j < 2; j++ {
			blockMaybe := nextBlocks[j]
			origBlockInfo := origBlockInfos[j]

			if blockMaybe != nil {
				eql := true
				for k := 0; k < SlotCount; k++ {
					if blockMaybe[k].Slot != origBlockInfo.Block[k].Slot {
						eql = false
						break
					}
				}
				if eql {
					s := origBlockInfo.ParentSlot
					nextSlots[j] = &s
				} else {
					nextPtr, err := db.Core.Position()
					if err != nil {
						return LinkedArrayListHeader{}, err
					}
					var leafCount int64
					for k := 0; k < SlotCount; k++ {
						b := blockMaybe[k].ToBytes()
						if err := db.Core.Write(b[:]); err != nil {
							return LinkedArrayListHeader{}, err
						}
						if isLeafNode {
							if !blockMaybe[k].Slot.Empty() {
								leafCount++
							}
						} else {
							leafCount += blockMaybe[k].Size
						}
					}
					var full bool
					if j == 0 {
						full = true
					}
					s := LinkedArrayListSlot{
						Size: leafCount,
						Slot: Slot{Value: nextPtr, Tag: TagIndex, Full: full},
					}
					nextSlots[j] = &s
				}
			}
		}

		if nextSlots[0] != nil && nextSlots[1] == nil {
			break
		}
	}

	rootSlot := nextSlots[0]
	if rootSlot == nil {
		return LinkedArrayListHeader{}, ErrExpectedRootNode
	}

	return LinkedArrayListHeader{Shift: nextShift, Ptr: rootSlot.Slot.Value, Size: size}, nil
}

func (db *Database) readLinkedArrayListConcat(headerA, headerB LinkedArrayListHeader) (LinkedArrayListHeader, error) {
	// read the first list's blocks
	var blocksA []LinkedArrayListBlockInfo
	var keyA int64
	if headerA.Size == 0 {
		keyA = 0
	} else {
		keyA = headerA.Size - 1
	}
	if err := db.readLinkedArrayListBlocks(headerA.Ptr, keyA, headerA.Shift, &blocksA); err != nil {
		return LinkedArrayListHeader{}, err
	}

	// read the second list's blocks
	var blocksB []LinkedArrayListBlockInfo
	if err := db.readLinkedArrayListBlocks(headerB.Ptr, 0, headerB.Shift, &blocksB); err != nil {
		return LinkedArrayListHeader{}, err
	}

	nextSlots := [2]*LinkedArrayListSlot{nil, nil}
	var nextShift byte = 0
	maxLen := max(len(blocksA), len(blocksB))

	for i := 0; i < maxLen; i++ {
		var blockInfoA, blockInfoB *LinkedArrayListBlockInfo
		if i < len(blocksA) {
			b := blocksA[len(blocksA)-1-i]
			blockInfoA = &b
		}
		if i < len(blocksB) {
			b := blocksB[len(blocksB)-1-i]
			blockInfoB = &b
		}
		var nextBlocksList [2]*[SlotCount]LinkedArrayListSlot
		isLeafNode := nextSlots[0] == nil

		if !isLeafNode {
			nextShift++
		}

		blockInfos := [2]*LinkedArrayListBlockInfo{blockInfoA, blockInfoB}

		for j := 0; j < 2; j++ {
			bi := blockInfos[j]
			if bi != nil {
				var block [SlotCount]LinkedArrayListSlot
				populateArray(&block)
				targetI := 0
				for sourceI := 0; sourceI < SlotCount; sourceI++ {
					blockSlot := bi.Block[sourceI]
					if !isLeafNode && bi.I == byte(sourceI) {
						continue
					} else if blockSlot.Slot.Empty() {
						break
					}
					block[targetI] = blockSlot
					targetI++
				}
				if targetI == 0 {
					continue
				}
				nextBlocksList[j] = &block
			}
		}

		slotsToWrite := make([]LinkedArrayListSlot, SlotCount*2)
		for k := range slotsToWrite {
			slotsToWrite[k] = LinkedArrayListSlot{Size: 0, Slot: Slot{}}
		}
		slotI := 0

		if nextBlocksList[0] != nil {
			for _, bs := range nextBlocksList[0] {
				if bs.Slot.Empty() {
					break
				}
				slotsToWrite[slotI] = bs
				slotI++
			}
		}
		for _, sm := range nextSlots {
			if sm != nil {
				slotsToWrite[slotI] = *sm
				slotI++
			}
		}
		if nextBlocksList[1] != nil {
			for _, bs := range nextBlocksList[1] {
				if bs.Slot.Empty() {
					break
				}
				slotsToWrite[slotI] = bs
				slotI++
			}
		}

		nextSlots = [2]*LinkedArrayListSlot{nil, nil}

		var blocks [2][SlotCount]LinkedArrayListSlot
		populateArray(&blocks[0])
		populateArray(&blocks[1])

		if slotI > SlotCount {
			if headerA.Size < headerB.Size {
				for j := 0; j < slotI-SlotCount; j++ {
					blocks[0][j] = slotsToWrite[j]
				}
				for j := 0; j < SlotCount; j++ {
					blocks[1][j] = slotsToWrite[j+(slotI-SlotCount)]
				}
			} else {
				for j := 0; j < SlotCount; j++ {
					blocks[0][j] = slotsToWrite[j]
				}
				for j := 0; j < slotI-SlotCount; j++ {
					blocks[1][j] = slotsToWrite[j+SlotCount]
				}
			}
		} else {
			for j := 0; j < slotI; j++ {
				blocks[0][j] = slotsToWrite[j]
			}
		}

		coreLen, err := db.Core.Length()
		if err != nil {
			return LinkedArrayListHeader{}, err
		}
		if err := db.Core.SeekTo(coreLen); err != nil {
			return LinkedArrayListHeader{}, err
		}

		for blockI := 0; blockI < 2; blockI++ {
			block := blocks[blockI]
			if block[0].Slot.Empty() {
				break
			}

			nextPtr, err := db.Core.Position()
			if err != nil {
				return LinkedArrayListHeader{}, err
			}
			var leafCount int64
			for _, bs := range block {
				b := bs.ToBytes()
				if err := db.Core.Write(b[:]); err != nil {
					return LinkedArrayListHeader{}, err
				}
				if isLeafNode {
					if !bs.Slot.Empty() {
						leafCount++
					}
				} else {
					leafCount += bs.Size
				}
			}

			s := LinkedArrayListSlot{Size: leafCount, Slot: Slot{Value: nextPtr, Tag: TagIndex, Full: true}}
			nextSlots[blockI] = &s
		}
	}

	var rootPtr int64
	if nextSlots[0] != nil {
		if nextSlots[1] != nil {
			var block [SlotCount]LinkedArrayListSlot
			populateArray(&block)
			block[0] = *nextSlots[0]
			block[1] = *nextSlots[1]

			newPtr, err := db.Core.Length()
			if err != nil {
				return LinkedArrayListHeader{}, err
			}
			for _, bs := range block {
				b := bs.ToBytes()
				if err := db.Core.Write(b[:]); err != nil {
					return LinkedArrayListHeader{}, err
				}
			}

			if nextShift == MaxBranchLength {
				return LinkedArrayListHeader{}, ErrMaxShiftExceeded
			}
			nextShift++
			rootPtr = newPtr
		} else {
			rootPtr = nextSlots[0].Slot.Value
		}
	} else {
		rootPtr = headerA.Ptr
	}

	return LinkedArrayListHeader{
		Shift: nextShift,
		Ptr:   rootPtr,
		Size:  headerA.Size + headerB.Size,
	}, nil
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
		newOffset, err := remapLinkedArrayList(sourceCore, targetCore, hashSize, offsetMap, slot)
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

func remapLinkedArrayList(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, slot Slot) (int64, error) {
	if err := sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}
	var headerBytes [LinkedArrayListHeaderLength]byte
	if err := sourceCore.Read(headerBytes[:]); err != nil {
		return 0, err
	}
	header, err := LinkedArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return 0, err
	}

	remappedPtr, err := remapLinkedArrayListBlock(sourceCore, targetCore, hashSize, offsetMap, header.Ptr)
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
	newHeader := LinkedArrayListHeader{Shift: header.Shift, Ptr: remappedPtr, Size: header.Size}
	nhb := newHeader.ToBytes()
	if err := targetCore.Write(nhb[:]); err != nil {
		return 0, err
	}

	return newOffset, nil
}

func remapLinkedArrayListBlock(sourceCore, targetCore Core, hashSize uint16, offsetMap map[int64]int64, blockOffset int64) (int64, error) {
	if mapped, ok := offsetMap[blockOffset]; ok {
		return mapped, nil
	}

	if err := sourceCore.SeekTo(blockOffset); err != nil {
		return 0, err
	}
	blockBytes := make([]byte, LinkedArrayListIndexBlockSize)
	if err := sourceCore.Read(blockBytes); err != nil {
		return 0, err
	}

	var slots [SlotCount]LinkedArrayListSlot
	for i := 0; i < SlotCount; i++ {
		s, err := LinkedArrayListSlotFromBytes(blockBytes[i*LinkedArrayListSlotLength : (i+1)*LinkedArrayListSlotLength])
		if err != nil {
			return 0, err
		}
		slots[i] = s
	}

	var remappedSlots [SlotCount]LinkedArrayListSlot
	for i := 0; i < SlotCount; i++ {
		s := slots[i]
		if s.Slot.Tag == TagIndex {
			remappedPtr, err := remapLinkedArrayListBlock(sourceCore, targetCore, hashSize, offsetMap, s.Slot.Value)
			if err != nil {
				return 0, err
			}
			remappedSlots[i] = LinkedArrayListSlot{Size: s.Size, Slot: Slot{Value: remappedPtr, Tag: TagIndex, Full: s.Slot.Full}}
		} else if s.Slot.Empty() {
			remappedSlots[i] = s
		} else {
			remapped, err := remapSlot(sourceCore, targetCore, hashSize, offsetMap, s.Slot)
			if err != nil {
				return 0, err
			}
			remappedSlots[i] = LinkedArrayListSlot{Size: s.Size, Slot: remapped}
		}
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

	offsetMap[blockOffset] = newOffset
	return newOffset, nil
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
