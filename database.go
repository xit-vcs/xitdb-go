package xitdb

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"math"
	"math/big"
)

const (
	Version         uint16 = 1
	BitCount               = 4
	SlotCount              = 1 << BitCount
	Mask            int64  = SlotCount - 1
	IndexBlockSize         = SlotLength * SlotCount
	MaxBranchLength        = 16
	// b-tree (backs LinkedArrayList): nodes hold up to BTreeSlotCount entries
	BTreeSlotCount       = SlotCount
	BTreeSplitCount      = (BTreeSlotCount + 1) / 2
	BTreeNodeHeaderSize  = 2
	BTreeLeafBlockSize   = BTreeNodeHeaderSize + SlotLength*BTreeSlotCount
	BTreeBranchBlockSize = BTreeNodeHeaderSize + (SlotLength+8)*BTreeSlotCount
	// sorted_map / sorted_set node block: a leaf holds BTreeSlotCount entry slots;
	// a branch holds child slots, separator slots, then BTreeSlotCount u64 counts
	SortedLeafBlockSize   = BTreeNodeHeaderSize + SlotLength*BTreeSlotCount
	SortedBranchBlockSize = BTreeNodeHeaderSize + (SlotLength*2+8)*BTreeSlotCount
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

const HeaderLength = 20

type Header struct {
	// a random ID that never changes. it is part of every trailer's checksum,
	// so a replica can't be extended with bytes from a different file.
	FileID   uint64
	HashID   uint32
	HashSize uint16
	// the file format version
	Version     uint16
	MagicNumber [3]byte
}

func (h Header) ToBytes() [HeaderLength]byte {
	var buf [HeaderLength]byte
	copy(buf[0:3], h.MagicNumber[:])
	// buf[3] is currently unused padding
	binary.BigEndian.PutUint16(buf[4:6], h.Version)
	binary.BigEndian.PutUint16(buf[6:8], h.HashSize)
	binary.BigEndian.PutUint32(buf[8:12], h.HashID)
	binary.BigEndian.PutUint64(buf[12:20], h.FileID)
	return buf
}

func ReadHeader(c Core) (Header, error) {
	var magicNumber [3]byte
	if err := c.Read(magicNumber[:]); err != nil {
		return Header{}, err
	}
	// currently unused padding
	if _, err := readByte(c); err != nil {
		return Header{}, err
	}
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
	fileID, err := readLong(c)
	if err != nil {
		return Header{}, err
	}
	return Header{
		FileID:      uint64(fileID),
		HashID:      hashID,
		HashSize:    hashSize,
		Version:     version,
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
	if h.Version != Version {
		return ErrInvalidVersion
	}
	return nil
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

// RootHeader

// the database header is followed by the root header. the top-level data
// is a root slot of any type, and every write to it is a copy-on-write
// transaction that appends a trailer holding the new root slot. the root
// header only caches the latest trailer, so every other committed byte is
// immutable, and a replica stays in sync by copying the bytes it doesn't have.
const (
	RootHeaderStart = HeaderLength
	RootHeaderEnd   = RootHeaderStart + RootHeaderLength
)

// rewritten at every commit, as a cache of the latest trailer.
// the root slot comes first on disk.
const RootHeaderLength = SlotLength + 8

type RootHeader struct {
	FileSize int64
	Root     Slot
}

func (h RootHeader) ToBytes() [RootHeaderLength]byte {
	var buf [RootHeaderLength]byte
	root := h.Root.ToBytes()
	copy(buf[0:SlotLength], root[:])
	binary.BigEndian.PutUint64(buf[SlotLength:], uint64(h.FileSize))
	return buf
}

func RootHeaderFromBytes(b []byte) RootHeader {
	var root [SlotLength]byte
	copy(root[:], b[0:SlotLength])
	return RootHeader{
		FileSize: int64(binary.BigEndian.Uint64(b[SlotLength : SlotLength+8])),
		Root:     SlotFromBytes(root),
	}
}

// RootTrailer

// appended at the end of every transaction, so the committed bytes (up to
// the root header's FileSize) always end with one. the root slot comes first
// on disk.
const RootTrailerLength = SlotLength + 16

type RootTrailer struct {
	Checksum uint64
	// random for every transaction except the first, so separately written
	// copies of a file can't end up with the same trailers, even when they
	// write data of the same size. that is how a replica notices it is being
	// sent bytes from a different history.
	CommitID uint64
	Root     Slot
}

func NewRootTrailer(fileID uint64, position int64, root Slot, commitID uint64) RootTrailer {
	return RootTrailer{Checksum: computeRootTrailerChecksum(fileID, position, root, commitID), CommitID: commitID, Root: root}
}

func (t RootTrailer) ToBytes() [RootTrailerLength]byte {
	var buf [RootTrailerLength]byte
	root := t.Root.ToBytes()
	copy(buf[0:SlotLength], root[:])
	binary.BigEndian.PutUint64(buf[SlotLength:SlotLength+8], t.CommitID)
	binary.BigEndian.PutUint64(buf[SlotLength+8:], t.Checksum)
	return buf
}

func RootTrailerFromBytes(b []byte) RootTrailer {
	var root [SlotLength]byte
	copy(root[:], b[0:SlotLength])
	return RootTrailer{
		Checksum: binary.BigEndian.Uint64(b[SlotLength+8 : SlotLength+16]),
		CommitID: binary.BigEndian.Uint64(b[SlotLength : SlotLength+8]),
		Root:     SlotFromBytes(root),
	}
}

var crc64Table = crc64.MakeTable(crc64.ECMA)

// CRC-64/XZ of the file ID, the trailer's position, root, and the commit ID,
// all big endian. including the file ID and position means the trailer can't
// be mistaken for one written elsewhere. 64 bits makes it safe to find the
// last trailer by scanning a file whose root header can't be trusted.
func computeRootTrailerChecksum(fileID uint64, position int64, root Slot, commitID uint64) uint64 {
	var buf [24 + SlotLength]byte
	binary.BigEndian.PutUint64(buf[0:8], fileID)
	binary.BigEndian.PutUint64(buf[8:16], uint64(position))
	slotBytes := root.ToBytes()
	copy(buf[16:16+SlotLength], slotBytes[:])
	binary.BigEndian.PutUint64(buf[16+SlotLength:], commitID)
	// the ECMA table with crc64's ones-complement init and xorout is CRC-64/XZ
	return crc64.Checksum(buf[:], crc64Table)
}

// ReplicaState

// what a replica sends its source, so the source knows which
// bytes to send back (see Database.ReplicationStart)
type ReplicaState struct {
	CommittedSize int64
	// the checksum of the trailer that ends the committed bytes. it covers the
	// file ID and the commit's random ID, so it identifies both the file and the
	// history that led to the commit.
	Checksum uint64
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

// SortedKeyValuePair

// a KeyValuePair without the hash, which sorted maps don't need. it is
// the key slot followed by the value slot.
const SortedKeyValuePairLength = SlotLength * 2

// where the key slot is in a TagKVPair or TagSortedKVPair entry. the value slot follows it.
func (db *Database) kvKeyOffset(tag Tag) int64 {
	if tag == TagKVPair {
		return int64(db.Header.HashSize)
	}
	return 0
}

// decodes a TagKVPair or TagSortedKVPair entry at the core's position.
// a TagSortedKVPair has no hash, so it is all zeros.
func decodeKvPair(core Core, tag Tag, hashSize int) (KeyValuePair, error) {
	var kvPair KeyValuePair
	switch tag {
	case TagKVPair:
		b := make([]byte, KeyValuePairLength(hashSize))
		if err := core.Read(b); err != nil {
			return KeyValuePair{}, err
		}
		kvPair = KeyValuePairFromBytes(b, hashSize)
	case TagSortedKVPair:
		var b [SortedKeyValuePairLength]byte
		if err := core.Read(b[:]); err != nil {
			return KeyValuePair{}, err
		}
		kvPair = KeyValuePairFromBytes(b[:], 0)
		kvPair.Hash = make([]byte, hashSize)
	default:
		return KeyValuePair{}, ErrUnexpectedTag
	}
	if err := kvPair.KeySlot.Tag.validate(); err != nil {
		return KeyValuePair{}, err
	}
	if err := kvPair.ValueSlot.Tag.validate(); err != nil {
		return KeyValuePair{}, err
	}
	return kvPair, nil
}

// encodes a TagKVPair or TagSortedKVPair entry at the core's position.
// a TagSortedKVPair leaves out the hash.
func encodeKvPair(core Core, tag Tag, kvPair KeyValuePair) error {
	switch tag {
	case TagKVPair:
		return core.Write(kvPair.ToBytes())
	case TagSortedKVPair:
		return core.Write(KeyValuePair{ValueSlot: kvPair.ValueSlot, KeySlot: kvPair.KeySlot}.ToBytes())
	default:
		return ErrUnexpectedTag
	}
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

// SortedNode: a count-augmented B+tree keyed on byte strings, ordered
// lexicographically. a leaf holds .sorted_kv_pair entries in ascending key
// order; a branch holds child slots, separator slots (the smallest key in each
// child's subtree; Separators[0] is an unused sentinel), and per-child subtree
// counts.

type SortedNode struct {
	Kind       BTreeNodeKind
	Num        int
	Entries    [BTreeSlotCount]Slot  // leaf
	Children   [BTreeSlotCount]Slot  // branch
	Separators [BTreeSlotCount]Slot  // branch
	Counts     [BTreeSlotCount]int64 // branch
}

func (n *SortedNode) SubtreeCount() int64 {
	if n.Kind == BTreeKindLeaf {
		return int64(n.Num)
	}
	var total int64
	for i := 0; i < n.Num; i++ {
		total += n.Counts[i]
	}
	return total
}

// the new right sibling produced when a node splits
type SortedSplit struct {
	NodePtr   int64
	Count     int64
	Separator Slot
}

// insert/replace result: the entry whose value to write, whether a new entry
// was added (vs replacing), and the new right sibling if this node split
type SortedInsertResult struct {
	NodePtr int64
	Count   int64
	KvSlot  Slot
	Added   bool
	Split   *SortedSplit
}

type SortedRemoveResult struct {
	NodePtr int64
	Found   bool
}

type SortedSlot struct {
	Slot     Slot
	Position int64
}

type SortedEntry struct {
	KvSlot  Slot
	KeySlot Slot
}

// SortedMapGetTarget

type SortedMapGetTarget interface {
	sortedMapGetTarget()
	getKey() []byte
}

type SortedMapGetKVPair struct{ Key []byte }
type SortedMapGetKey struct{ Key []byte }
type SortedMapGetValue struct{ Key []byte }

func (SortedMapGetKVPair) sortedMapGetTarget() {}
func (SortedMapGetKey) sortedMapGetTarget()    {}
func (SortedMapGetValue) sortedMapGetTarget()  {}
func (t SortedMapGetKVPair) getKey() []byte    { return t.Key }
func (t SortedMapGetKey) getKey() []byte       { return t.Key }
func (t SortedMapGetValue) getKey() []byte     { return t.Key }

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

type transaction struct {
	rootPosition *int64
	// the copy of the root slot that the transaction works on
	scratchPosition *int64
	frozenAt        *int64
}

// whether position is the transaction's root, which stays writable after freezing
func (t *transaction) isRootPosition(position *int64) bool {
	return position != nil && t.rootPosition != nil && *position == *t.rootPosition
}

// whether position is the copy of the root slot that the transaction works on
func (t *transaction) isScratchPosition(position *int64) bool {
	return position != nil && t.scratchPosition != nil && *position == *t.scratchPosition
}

type Database struct {
	Core        Core
	newHash     func() hash.Hash
	Header      Header
	TxStart     *int64
	transaction *transaction
}

func NewDatabase(core Core, hasher Hasher) (*Database, error) {
	db := &Database{
		Core:    core,
		newHash: hasher.Hash,
	}

	if err := core.SeekTo(0); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}
	length, err := core.Length()
	if err != nil {
		return nil, fmt.Errorf("length: %w", err)
	}

	if length == 0 {
		fileID := hasher.FileID
		if fileID == 0 {
			fileID, err = randomUint64()
			if err != nil {
				return nil, fmt.Errorf("generate file id: %w", err)
			}
		}
		digestLen := uint16(db.newHash().Size())
		db.Header = Header{
			FileID:      fileID,
			HashID:      hasher.ID,
			HashSize:    digestLen,
			Version:     Version,
			MagicNumber: MagicNumber,
		}
		if err := db.Header.Write(core); err != nil {
			return nil, fmt.Errorf("write header: %w", err)
		}
		// write the root header and commit an empty root, so the
		// file ends with a trailer like after any transaction
		rootHeader := RootHeader{FileSize: 0, Root: Slot{}}.ToBytes()
		if err := core.Write(rootHeader[:]); err != nil {
			return nil, fmt.Errorf("write root header: %w", err)
		}
		// the first commit's ID is 0, so a new database's bytes only
		// depend on its file ID
		if err := db.commitRoot(Slot{}, 0); err != nil {
			return nil, fmt.Errorf("commit root: %w", err)
		}
		if err := core.Sync(); err != nil {
			return nil, fmt.Errorf("sync: %w", err)
		}
	} else {
		header, err := db.readAndValidateHeader()
		if err != nil {
			return nil, err
		}
		db.Header = header
		if _, err := db.CommittedSize(); err != nil {
			return nil, fmt.Errorf("validate committed size: %w", err)
		}
	}

	db.TxStart = nil
	return db, nil
}

func (db *Database) digest(data []byte) []byte {
	digest := db.newHash()
	digest.Write(data)
	return digest.Sum(nil)
}

func (db *Database) readAndValidateHeader() (Header, error) {
	if err := db.Core.SeekTo(0); err != nil {
		return Header{}, fmt.Errorf("seek: %w", err)
	}
	header, err := ReadHeader(db.Core)
	if err != nil {
		return Header{}, fmt.Errorf("read header: %w", err)
	}
	if err := header.Validate(); err != nil {
		return Header{}, fmt.Errorf("validate header: %w", err)
	}
	if header.HashSize != uint16(db.newHash().Size()) {
		return Header{}, ErrInvalidHashSize
	}
	return header, nil
}

func (db *Database) RootCursor() *WriteCursor {
	// the cursor only holds a snapshot of the root slot, and every
	// operation that starts at it reads the root again. if this read
	// fails, the cursor looks empty and the next operation returns the error.
	root, err := db.readRootSlot()
	if err != nil {
		root = Slot{}
	}
	position := int64(RootHeaderStart)
	rc := &ReadCursor{
		SlotPtr: SlotPointer{Position: &position, Slot: root},
		DB:      db,
	}
	return &WriteCursor{ReadCursor: rc}
}

// runs fn with a write cursor to the root in a single transaction,
// discarding everything it wrote if it returns an error. use it to group
// writes to a root that isn't an array list, such as a hash map. for an
// array list root, WriteArrayList.AppendContext does the same.
func (db *Database) RunContext(fn ContextFunction) error {
	_, err := db.RootCursor().WritePath([]PathPart{Context{Function: fn}})
	return err
}

func (db *Database) Freeze() error {
	active := db.transaction
	if active == nil || db.TxStart == nil {
		return ErrExpectedTxStart
	}
	length, err := db.Core.Length()
	if err != nil {
		return err
	}
	db.TxStart = &length
	active.frozenAt = db.TxStart
	return nil
}

// this database's ReplicaState.
func (db *Database) ReplicaState() (ReplicaState, error) {
	committedSize, err := db.CommittedSize()
	if err != nil {
		return ReplicaState{}, err
	}
	trailer, err := db.readRootTrailer(committedSize)
	if err != nil {
		return ReplicaState{}, err
	}
	return ReplicaState{CommittedSize: committedSize, Checksum: trailer.Checksum}, nil
}

// where to start sending committed bytes to a replica in state. that's its
// committed size if its bytes are a prefix of this database, or 0 if it must
// be replaced, because it's a replica of another file or it has diverged
// from this one.
func (db *Database) ReplicationStart(state ReplicaState) (int64, error) {
	committedSize, err := db.CommittedSize()
	if err != nil {
		return 0, err
	}
	if state.CommittedSize > committedSize {
		return 0, nil
	}
	trailer, err := db.readRootTrailer(state.CommittedSize)
	if err != nil {
		if errors.Is(err, ErrInvalidDatabase) || errors.Is(err, ErrUnexpectedTag) {
			return 0, nil
		}
		return 0, err
	}
	if trailer.Checksum == state.Checksum {
		return state.CommittedSize, nil
	}
	return 0, nil
}

// writes the committed bytes from start up to end to w. start is usually
// from ReplicationStart, and end can't be past CommittedSize. committed bytes
// never change except for the root header, so when start is 0, a root header
// matching end is written, making the bytes a complete database by themselves.
func (db *Database) ReadCommitted(start, end int64, w io.Writer) error {
	committedSize, err := db.CommittedSize()
	if err != nil {
		return err
	}
	if start < 0 || start > end || end > committedSize {
		return ErrInvalidRange
	}
	if start > 0 && start < RootHeaderEnd {
		return ErrInvalidRange
	}
	if start == 0 {
		trailer, err := db.readRootTrailer(end)
		if err != nil {
			return err
		}
		if err := db.streamBytes(w, 0, RootHeaderStart); err != nil {
			return err
		}
		rootHeader := RootHeader{FileSize: end, Root: trailer.Root}.ToBytes()
		if _, err := w.Write(rootHeader[:]); err != nil {
			return err
		}
		return db.streamBytes(w, RootHeaderEnd, end-RootHeaderEnd)
	}
	return db.streamBytes(w, start, end-start)
}

// writes length bytes, starting at start, to w
func (db *Database) streamBytes(w io.Writer, start, length int64) error {
	if err := db.Core.SeekTo(start); err != nil {
		return err
	}
	buf := make([]byte, min(length, 4096))
	for length > 0 {
		chunk := min(length, int64(len(buf)))
		if err := db.Core.Read(buf[:chunk]); err != nil {
			return err
		}
		if _, err := w.Write(buf[:chunk]); err != nil {
			return err
		}
		length -= chunk
	}
	return nil
}

// writes length bytes from r at start and commits them. they must be the
// bytes from ReadCommitted, so they end with a trailer. when start is 0, this
// database is replaced by them; otherwise start must be this database's
// CommittedSize.
func (db *Database) ApplyReplicated(start int64, r io.Reader, length int64) error {
	if start == 0 {
		if err := db.Core.SetLength(0); err != nil {
			return err
		}
	} else {
		committedSize, err := db.CommittedSize()
		if err != nil {
			return err
		}
		if start != committedSize {
			return ErrInvalidRange
		}
		// discard anything past the committed bytes, such as the remains of an interrupted sync
		if err := db.truncate(); err != nil {
			return err
		}
	}

	if err := db.Core.SeekTo(start); err != nil {
		return err
	}
	buf := make([]byte, min(max(length, 0), 4096))
	for remaining := length; remaining > 0; {
		chunk := min(remaining, int64(len(buf)))
		if _, err := io.ReadFull(r, buf[:chunk]); err != nil {
			return err
		}
		if err := db.Core.Write(buf[:chunk]); err != nil {
			return err
		}
		remaining -= chunk
	}

	if start == 0 {
		// the bytes are a different database, with its own header
		if err := db.Core.Sync(); err != nil {
			return err
		}
		header, err := db.readAndValidateHeader()
		if err != nil {
			return err
		}
		db.Header = header
		if _, err := db.CommittedSize(); err != nil {
			return err
		}
	} else if length > 0 {
		// the bytes must end with a trailer. update the root header to match it.
		trailer, err := db.readRootTrailer(start + length)
		if err != nil {
			return err
		}
		if err := db.Core.Sync(); err != nil {
			return err
		}
		if err := db.writeRootHeader(trailer.Root); err != nil {
			return err
		}
		if err := db.Core.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// resets and borrows the supplied offsets map without closing it.
//
// the target must be a new or empty database, or this returns
// ErrTargetNotEmpty.
func (db *Database) Compact(targetCore Core, offsetMap OffsetMap) (*Database, error) {
	// cached offsets only apply to this compaction's target
	if err := offsetMap.Reset(); err != nil {
		return nil, err
	}
	hasher := Hasher{Hash: db.newHash, ID: db.Header.HashID}
	target, err := NewDatabase(targetCore, hasher)
	if err != nil {
		return nil, fmt.Errorf("init target: %w", err)
	}

	// the target must be a new or empty database, because compaction
	// writes to it with ordinary transactions. this also rejects
	// compacting a database into its own file.
	targetRoot, err := target.readRootSlot()
	if err != nil {
		return nil, fmt.Errorf("read target root: %w", err)
	}
	if !targetRoot.Empty() {
		return nil, ErrTargetNotEmpty
	}

	sourceRoot, err := db.readRootSlot()
	if err != nil {
		return nil, fmt.Errorf("read source root: %w", err)
	}

	// an empty root compacts to a new database. a root explicitly
	// set to null isn't empty, so it is remapped below.
	if sourceRoot.Empty() {
		return target, nil
	}

	// the target is written as one ordinary transaction, so it is committed
	// and fsynced like any other (callers typically rename it over an
	// existing database file). the remap must run inside the transaction,
	// because starting one truncates the target.
	compactor := compactor{
		sourceCore: db.Core,
		targetCore: target.Core,
		hashSize:   db.Header.HashSize,
		offsetMap:  offsetMap,
	}
	remap := func(slot Slot) ContextFunction {
		return func(cursor *WriteCursor) error {
			remapped, err := compactor.remapSlot(slot)
			if err != nil {
				return fmt.Errorf("remap: %w", err)
			}
			return cursor.Write(remapped)
		}
	}

	switch sourceRoot.Tag {
	case TagArrayList:
		// an array list root is a history, and its latest moment
		// becomes the only moment of a new one
		lastMoment, err := db.RootCursor().ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			return nil, fmt.Errorf("read last moment: %w", err)
		}
		if !lastMoment.Empty() {
			if _, err := target.RootCursor().WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				Context{Function: remap(lastMoment)},
			}); err != nil {
				return nil, err
			}
		}
	default:
		// any other root is remapped whole
		if err := target.RunContext(remap(sourceRoot)); err != nil {
			return nil, err
		}
	}

	return target, nil
}

// committed size

// the number of bytes that belong to committed transactions. they never
// change except for the root header (see ReadCommitted).
func (db *Database) CommittedSize() (int64, error) {
	header, err := db.readRootHeader()
	if err != nil {
		return 0, err
	}
	minimumSize := int64(RootHeaderEnd + RootTrailerLength)
	if header.FileSize < minimumSize {
		return 0, ErrInvalidDatabase
	}
	fileSize, err := db.Core.Length()
	if err != nil {
		return 0, err
	}
	if fileSize < header.FileSize {
		return 0, ErrTruncatedDatabase
	}
	// the committed bytes must end with a trailer matching the header
	trailer, err := db.readRootTrailer(header.FileSize)
	if err != nil {
		return 0, err
	}
	if trailer.Root != header.Root {
		return 0, ErrInvalidDatabase
	}
	return header.FileSize, nil
}

func (db *Database) truncate() error {
	committedSize, err := db.CommittedSize()
	if err != nil {
		return err
	}
	active := db.transaction
	if active != nil && active.frozenAt != nil && *active.frozenAt > committedSize {
		// retain frozen bytes without publishing the failed transaction.
		// discard what follows the frozen bytes, then commit the current
		// root again after them, so they end with a trailer
		fileSize, err := db.Core.Length()
		if err != nil {
			return err
		}
		if fileSize > *active.frozenAt {
			if err := db.Core.SetLength(*active.frozenAt); err != nil {
				return err
			}
		}
		root, err := db.readRootSlot()
		if err != nil {
			return err
		}
		commitID, err := randomUint64()
		if err != nil {
			return err
		}
		if err := db.commitRoot(root, commitID); err != nil {
			return err
		}
		return db.Core.Sync()
	}
	fileSize, err := db.Core.Length()
	if err != nil {
		return err
	}
	if fileSize > committedSize {
		return db.Core.SetLength(committedSize)
	}
	return nil
}

func (db *Database) readRootHeader() (RootHeader, error) {
	if err := db.Core.SeekTo(RootHeaderStart); err != nil {
		return RootHeader{}, err
	}
	var b [RootHeaderLength]byte
	if err := db.Core.Read(b[:]); err != nil {
		return RootHeader{}, err
	}
	header := RootHeaderFromBytes(b[:])
	if err := header.Root.Tag.validate(); err != nil {
		return RootHeader{}, err
	}
	return header, nil
}

// the most recently committed root slot
func (db *Database) readRootSlot() (Slot, error) {
	header, err := db.readRootHeader()
	if err != nil {
		return Slot{}, err
	}
	return header.Root, nil
}

// the trailer that ends at end
func (db *Database) readRootTrailer(end int64) (RootTrailer, error) {
	if end < RootTrailerLength {
		return RootTrailer{}, ErrInvalidDatabase
	}
	trailerPos := end - RootTrailerLength
	if err := db.Core.SeekTo(trailerPos); err != nil {
		return RootTrailer{}, err
	}
	var b [RootTrailerLength]byte
	if err := db.Core.Read(b[:]); err != nil {
		return RootTrailer{}, err
	}
	trailer := RootTrailerFromBytes(b[:])
	if trailer.Checksum != computeRootTrailerChecksum(db.Header.FileID, trailerPos, trailer.Root, trailer.CommitID) {
		return RootTrailer{}, ErrInvalidDatabase
	}
	if err := trailer.Root.Tag.validate(); err != nil {
		return RootTrailer{}, err
	}
	return trailer, nil
}

// for file IDs and commit IDs
func randomUint64() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

// completes a transaction by appending a trailer for root and updating
// the root header. callers must sync afterwards to make the header itself
// durable.
func (db *Database) commitRoot(root Slot, commitID uint64) error {
	trailerPos, err := db.Core.Length()
	if err != nil {
		return err
	}
	if err := db.Core.SeekTo(trailerPos); err != nil {
		return err
	}
	trailer := NewRootTrailer(db.Header.FileID, trailerPos, root, commitID).ToBytes()
	if err := db.Core.Write(trailer[:]); err != nil {
		return err
	}

	// flush and fsync before updating the header, because updating
	// the header is what completes the transaction. without the
	// fsync, the OS could persist the header before the data it
	// points to, so a crash could commit a transaction whose data
	// never reached disk.
	if err := db.Core.Sync(); err != nil {
		return err
	}

	return db.writeRootHeader(root)
}

// points the root header at root, which must be in the trailer that ends
// the file.
func (db *Database) writeRootHeader(root Slot) error {
	fileSize, err := db.Core.Length()
	if err != nil {
		return err
	}
	if err := db.Core.SeekTo(RootHeaderStart); err != nil {
		return err
	}
	header := RootHeader{FileSize: fileSize, Root: root}.ToBytes()
	return db.Core.Write(header[:])
}

// whether this slot pointer is the root of the database
func isRoot(slotPtr SlotPointer) bool {
	return slotPtr.Position != nil && *slotPtr.Position == RootHeaderStart
}

// the collection tag that an init path part creates, if any
func initTag(part PathPart) (Tag, bool) {
	switch p := part.(type) {
	case ArrayListInit, *ArrayListInit:
		return TagArrayList, true
	case LinkedArrayListInit, *LinkedArrayListInit:
		return TagLinkedArrayList, true
	case HashMapInitPart:
		return hashMapInitTag(p), true
	case *HashMapInitPart:
		return hashMapInitTag(*p), true
	case SortedMapInitPart:
		return sortedMapInitTag(p), true
	case *SortedMapInitPart:
		return sortedMapInitTag(*p), true
	default:
		return TagNone, false
	}
}

func hashMapInitTag(p HashMapInitPart) Tag {
	if p.Counted {
		if p.Set {
			return TagCountedHashSet
		}
		return TagCountedHashMap
	}
	if p.Set {
		return TagHashSet
	}
	return TagHashMap
}

func sortedMapInitTag(p SortedMapInitPart) Tag {
	if p.Set {
		return TagSortedSet
	}
	return TagSortedMap
}

// the init path part for a collection tag, if any
func initPartFor(tag Tag) (PathPart, bool) {
	switch tag {
	case TagArrayList:
		return ArrayListInit{}, true
	case TagLinkedArrayList:
		return LinkedArrayListInit{}, true
	case TagHashMap:
		return HashMapInitPart{Counted: false, Set: false}, true
	case TagHashSet:
		return HashMapInitPart{Counted: false, Set: true}, true
	case TagCountedHashMap:
		return HashMapInitPart{Counted: true, Set: false}, true
	case TagCountedHashSet:
		return HashMapInitPart{Counted: true, Set: true}, true
	case TagSortedMap:
		return SortedMapInitPart{Set: false}, true
	case TagSortedSet:
		return SortedMapInitPart{Set: true}, true
	default:
		return nil, false
	}
}

// whether the data at ptr predates this transaction, so it must be
// copied before it is modified. writes only happen inside a transaction.
func (db *Database) isOld(ptr int64) (bool, error) {
	if db.TxStart == nil {
		return false, ErrExpectedTxStart
	}
	return ptr < *db.TxStart, nil
}

// copies an index block to the end unless it was made in this transaction
func (db *Database) copyIndexBlockIfOld(ptr int64) (int64, error) {
	old, err := db.isOld(ptr)
	if err != nil {
		return 0, err
	}
	if !old {
		return ptr, nil
	}
	indexBlock := make([]byte, IndexBlockSize)
	if err := db.Core.SeekTo(ptr); err != nil {
		return 0, err
	}
	if err := db.Core.Read(indexBlock); err != nil {
		return 0, err
	}
	nextPtr, err := db.Core.Length()
	if err != nil {
		return 0, err
	}
	if err := db.Core.SeekTo(nextPtr); err != nil {
		return 0, err
	}
	if err := db.Core.Write(indexBlock); err != nil {
		return 0, err
	}
	return nextPtr, nil
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

// reject writes into frozen data; the current transaction's root slot remains writable
func (db *Database) checkFrozenSlot(slotPtr SlotPointer) error {
	active := db.transaction
	if active != nil && active.frozenAt != nil && slotPtr.Position != nil &&
		*slotPtr.Position < *active.frozenAt && !active.isRootPosition(slotPtr.Position) {
		return ErrFrozenSlot
	}
	return nil
}

// copy frozen collection storage before mutation to preserve existing readers
func (db *Database) copyCollectionIfFrozen(slotPtr SlotPointer, init PathPart) (SlotPointer, error) {
	if db.transaction != nil && db.transaction.frozenAt != nil && slotPtr.Slot.Value < *db.transaction.frozenAt {
		return init.readSlotPointer(db, ReadWrite, []PathPart{init}, 0, slotPtr)
	}
	return slotPtr, nil
}

// readSlotPointer - the central path-traversal method

func (db *Database) readSlotPointer(writeMode WriteMode, path []PathPart, pathI int, slotPtr SlotPointer) (SlotPointer, error) {
	if pathI == len(path) {
		if writeMode == ReadOnly && slotPtr.Slot.Empty() {
			return SlotPointer{}, ErrKeyNotFound
		}
		return slotPtr, nil
	}
	if writeMode == ReadWrite {
		if err := db.checkFrozenSlot(slotPtr); err != nil {
			return SlotPointer{}, err
		}
	}
	part := path[pathI]

	// every write that starts at the root is a transaction
	if isRoot(slotPtr) {
		root, err := db.readRootSlot()
		if err != nil {
			return SlotPointer{}, err
		}

		// the position of a read path doesn't matter, but it must not be
		// RootHeaderStart or this would recurse forever
		if writeMode == ReadOnly {
			return db.readSlotPointer(writeMode, path, pathI, SlotPointer{Position: nil, Slot: root})
		}

		// initializing an initialized root is free, so a wrapper like
		// WriteArrayList can hold a root cursor without a transaction. like
		// starting one, it discards data left by an unfinished transaction.
		// (WritePath rejects root writes inside a transaction, so none
		// is open here.)
		if len(path)-pathI == 1 && root.Tag != TagNone {
			if tag, ok := initTag(part); ok && tag == root.Tag {
				if err := db.truncate(); err != nil {
					return SlotPointer{}, err
				}
				position := int64(RootHeaderStart)
				return SlotPointer{Position: &position, Slot: root}, nil
			}
		}

		// discard data left by an unfinished transaction after a crash
		if err := db.truncate(); err != nil {
			return SlotPointer{}, err
		}
		scratchPos, err := db.Core.Length()
		if err != nil {
			return SlotPointer{}, err
		}
		txStart := scratchPos
		db.TxStart = &txStart
		defer func() { db.TxStart = nil }()
		// the copy is the transaction's root, so it stays writable after freezing
		if db.transaction != nil {
			db.transaction.rootPosition = &scratchPos
			db.transaction.scratchPosition = &scratchPos
		}

		// the transaction works on a copy of the root slot
		if err := db.Core.SeekTo(scratchPos); err != nil {
			return SlotPointer{}, err
		}
		rootBytes := root.ToBytes()
		if err := db.Core.Write(rootBytes[:]); err != nil {
			return SlotPointer{}, err
		}
		rootPtr := SlotPointer{Position: &scratchPos, Slot: root}

		// every collection expects its init part to copy its header or root
		// block into the transaction before anything writes to it. the root
		// has no enclosing init part, so run one here.
		if initPart, ok := initPartFor(root.Tag); ok {
			rootPtr, err = db.readSlotPointer(writeMode, []PathPart{initPart}, 0, rootPtr)
			if err != nil {
				return SlotPointer{}, err
			}
		}

		result, err := db.readSlotPointer(writeMode, path, pathI, rootPtr)
		if err != nil {
			return SlotPointer{}, err
		}

		// the copy now holds the new root, so commit it
		if err := db.Core.SeekTo(scratchPos); err != nil {
			return SlotPointer{}, err
		}
		var nextRootBytes [SlotLength]byte
		if err := db.Core.Read(nextRootBytes[:]); err != nil {
			return SlotPointer{}, err
		}
		nextRoot := SlotFromBytes(nextRootBytes)
		commitID, err := randomUint64()
		if err != nil {
			return SlotPointer{}, err
		}
		if err := db.commitRoot(nextRoot, commitID); err != nil {
			return SlotPointer{}, err
		}

		// a path that ends at the root (such as an init or a write) returns
		// a root cursor, so it can be used for later transactions
		if result.Position != nil && *result.Position == scratchPos {
			position := int64(RootHeaderStart)
			result = SlotPointer{Position: &position, Slot: nextRoot}
		}
		return result, nil
	}

	return part.readSlotPointer(db, writeMode, path, pathI, slotPtr)
}

// HashMap methods

func (db *Database) readMapSlot(indexPos int64, keyHash []byte, keyOffset byte, writeMode WriteMode, target HashMapGetTarget) (HashMapGetResult, error) {
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
		if writeMode == ReadWrite {
			var err error
			nextPtr, err = db.copyIndexBlockIfOld(ptr)
			if err != nil {
				return HashMapGetResult{}, err
			}
			if nextPtr != ptr {
				// make slot point to the copied block
				if err := db.Core.SeekTo(slotPos); err != nil {
					return HashMapGetResult{}, err
				}
				newSlot := Slot{Value: nextPtr, Tag: TagIndex}
				newSlotBytes := newSlot.ToBytes()
				if err := db.Core.Write(newSlotBytes[:]); err != nil {
					return HashMapGetResult{}, err
				}
			}
		}
		return db.readMapSlot(nextPtr, keyHash, keyOffset+1, writeMode, target)

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
			if writeMode == ReadWrite {
				old, err := db.isOld(ptr)
				if err != nil {
					return HashMapGetResult{}, err
				}
				if old {
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
			res, err := db.readMapSlot(nextIndexPos, keyHash, keyOffset+1, writeMode, target)
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

func (db *Database) removeMapSlot(indexPos int64, keyHash []byte, keyOffset byte) (Slot, error) {
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
		nextSlot, err = db.removeMapSlot(slot.Value, keyHash, keyOffset+1)
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

	// there was more than one used slot, or a single INDEX slot,
	// so we must keep this index block. update the slot in it,
	// copying the block first unless it belongs to this transaction.
	nextIndexPos, err := db.copyIndexBlockIfOld(indexPos)
	if err != nil {
		return Slot{}, err
	}
	if err := db.Core.SeekTo(nextIndexPos + int64(SlotLength*i)); err != nil {
		return Slot{}, err
	}
	nsb := nextSlot.ToBytes()
	if err := db.Core.Write(nsb[:]); err != nil {
		return Slot{}, err
	}
	return Slot{Value: nextIndexPos, Tag: TagIndex}, nil
}

// ArrayList methods

func (db *Database) readArrayListSlotAppend(header ArrayListHeader, writeMode WriteMode) (ArrayListAppendResult, error) {
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

	slotPtr, err := db.readArrayListSlot(indexPos, key, nextShift, writeMode)
	if err != nil {
		return ArrayListAppendResult{}, err
	}
	// clear values left by a rollback or slice
	slotPtr = slotPtr.WithSlot(Slot{})
	if slotPtr.Position == nil {
		return ArrayListAppendResult{}, ErrCursorNotWriteable
	}
	if err := db.Core.SeekTo(*slotPtr.Position); err != nil {
		return ArrayListAppendResult{}, err
	}
	b := slotPtr.Slot.ToBytes()
	if err := db.Core.Write(b[:]); err != nil {
		return ArrayListAppendResult{}, err
	}
	return ArrayListAppendResult{
		Header:  ArrayListHeader{Ptr: indexPos, Size: header.Size + 1},
		SlotPtr: slotPtr,
	}, nil
}

func (db *Database) readArrayListSlot(indexPos int64, key int64, shift byte, writeMode WriteMode) (SlotPointer, error) {
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
			if err := db.Core.SeekTo(slotPos); err != nil {
				return SlotPointer{}, err
			}
			newSlot := Slot{Value: nextIndexPos, Tag: TagIndex}
			nsb := newSlot.ToBytes()
			if err := db.Core.Write(nsb[:]); err != nil {
				return SlotPointer{}, err
			}
			return db.readArrayListSlot(nextIndexPos, key, shift-1, writeMode)
		}
	case TagIndex:
		nextPtr := ptr
		if writeMode == ReadWrite {
			var err error
			nextPtr, err = db.copyIndexBlockIfOld(ptr)
			if err != nil {
				return SlotPointer{}, err
			}
			if nextPtr != ptr {
				// make slot point to the copied block
				if err := db.Core.SeekTo(slotPos); err != nil {
					return SlotPointer{}, err
				}
				newSlot := Slot{Value: nextPtr, Tag: TagIndex}
				nsb := newSlot.ToBytes()
				if err := db.Core.Write(nsb[:]); err != nil {
					return SlotPointer{}, err
				}
			}
		}
		return db.readArrayListSlot(nextPtr, key, shift-1, writeMode)
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
	// the new root may still belong to a past moment. unlike child
	// nodes, root nodes are written directly, so copy it now.
	indexPos, err := db.copyIndexBlockIfOld(indexPos)
	if err != nil {
		return ArrayListHeader{}, err
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
// subtrees fall below it here. for an ephemeral (non-transactional) database there is no
// transaction, so everything is mutable in place until a concat first sets TxStart.
func (db *Database) btreeReusable(ptr int64) bool {
	// a transactional database outside a transaction can't reuse anything
	old, err := db.isOld(ptr)
	if err != nil {
		return false
	}
	return !old
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

// sorted_map / sorted_set

func (db *Database) readSortedNode(ptr int64) (SortedNode, error) {
	if err := db.Core.SeekTo(ptr); err != nil {
		return SortedNode{}, err
	}
	var headerBytes [BTreeNodeHeaderSize]byte
	if err := db.Core.Read(headerBytes[:]); err != nil {
		return SortedNode{}, err
	}
	kindInt := headerBytes[0]
	if kindInt > byte(BTreeKindBranch) {
		return SortedNode{}, ErrInvalidBTreeNodeKind
	}
	kind := BTreeNodeKind(kindInt)
	num := int(headerBytes[1])
	if num > BTreeSlotCount {
		return SortedNode{}, ErrInvalidBTreeNode
	}
	node := SortedNode{Kind: kind, Num: num}
	switch kind {
	case BTreeKindLeaf:
		body := make([]byte, SlotLength*BTreeSlotCount)
		if err := db.Core.Read(body); err != nil {
			return SortedNode{}, err
		}
		for i := 0; i < BTreeSlotCount; i++ {
			var sb [SlotLength]byte
			copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
			node.Entries[i] = SlotFromBytes(sb)
		}
	case BTreeKindBranch:
		body := make([]byte, (SlotLength*2+8)*BTreeSlotCount)
		if err := db.Core.Read(body); err != nil {
			return SortedNode{}, err
		}
		for i := 0; i < BTreeSlotCount; i++ {
			var sb [SlotLength]byte
			copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
			node.Children[i] = SlotFromBytes(sb)
		}
		sepOffset := SlotLength * BTreeSlotCount
		for i := 0; i < BTreeSlotCount; i++ {
			var sb [SlotLength]byte
			copy(sb[:], body[sepOffset+i*SlotLength:sepOffset+i*SlotLength+SlotLength])
			node.Separators[i] = SlotFromBytes(sb)
		}
		countsOffset := SlotLength * 2 * BTreeSlotCount
		for i := 0; i < BTreeSlotCount; i++ {
			node.Counts[i] = int64(binary.BigEndian.Uint64(body[countsOffset+i*8 : countsOffset+i*8+8]))
		}
	}
	return node, nil
}

func (db *Database) writeSortedNodeAt(node SortedNode, ptr int64) error {
	if err := db.Core.SeekTo(ptr); err != nil {
		return err
	}
	var bodySize int
	if node.Kind == BTreeKindLeaf {
		bodySize = SortedLeafBlockSize
	} else {
		bodySize = SortedBranchBlockSize
	}
	buf := make([]byte, bodySize)
	buf[0] = byte(node.Kind)
	buf[1] = byte(node.Num)
	off := BTreeNodeHeaderSize
	switch node.Kind {
	case BTreeKindLeaf:
		for i := 0; i < BTreeSlotCount; i++ {
			sb := node.Entries[i].ToBytes()
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
			sb := node.Separators[i].ToBytes()
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

func (db *Database) writeSortedNode(node SortedNode) (int64, error) {
	ptr, err := db.Core.Length()
	if err != nil {
		return 0, err
	}
	if err := db.writeSortedNodeAt(node, ptr); err != nil {
		return 0, err
	}
	return ptr, nil
}

// reuse oldPtr's position in place when it belongs to this transaction (mirrors
// btreeWriteNode / the TxStart path-copying model)
func (db *Database) sortedWriteNode(node SortedNode, oldPtr int64) (int64, error) {
	if db.btreeReusable(oldPtr) {
		if err := db.writeSortedNodeAt(node, oldPtr); err != nil {
			return 0, err
		}
		return oldPtr, nil
	}
	return db.writeSortedNode(node)
}

func (db *Database) readKvPair(kvSlot Slot) (KeyValuePair, error) {
	if err := db.Core.SeekTo(kvSlot.Value); err != nil {
		return KeyValuePair{}, err
	}
	return decodeKvPair(db.Core, kvSlot.Tag, int(db.Header.HashSize))
}

// writes a TagKVPair or TagSortedKVPair entry at the end and returns its slot
func (db *Database) writeKvPair(tag Tag, kvPair KeyValuePair) (Slot, error) {
	pos, err := db.Core.Length()
	if err != nil {
		return Slot{}, err
	}
	if err := db.Core.SeekTo(pos); err != nil {
		return Slot{}, err
	}
	if err := encodeKvPair(db.Core, tag, kvPair); err != nil {
		return Slot{}, err
	}
	return Slot{Value: pos, Tag: tag}, nil
}

// lexicographic comparison of the byte key stored at keySlot (a bytes or short_bytes
// slot) against the in-memory target. returns <0, 0, or >0.
func (db *Database) compareKey(keySlot Slot, target []byte) (int, error) {
	switch keySlot.Tag {
	case TagShortBytes:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(keySlot.Value))
		total := 8
		if keySlot.Full {
			total = 6
		}
		length := total
		for i := 0; i < total; i++ {
			if buf[i] == 0 {
				length = i
				break
			}
		}
		return bytes.Compare(buf[:length], target), nil
	case TagBytes:
		if err := db.Core.SeekTo(keySlot.Value); err != nil {
			return 0, err
		}
		length, err := readLong(db.Core)
		if err != nil {
			return 0, err
		}
		keyBytes := make([]byte, length)
		if err := db.Core.Read(keyBytes); err != nil {
			return 0, err
		}
		return bytes.Compare(keyBytes, target), nil
	default:
		return 0, ErrUnexpectedTag
	}
}

// descend by key to the matching leaf entry, or nil
func (db *Database) sortedGet(rootPtr int64, key []byte) (*SortedSlot, error) {
	nodePtr := rootPtr
	for {
		node, err := db.readSortedNode(nodePtr)
		if err != nil {
			return nil, err
		}
		if node.Kind == BTreeKindLeaf {
			for i := 0; i < node.Num; i++ {
				entry := node.Entries[i]
				kv, err := db.readKvPair(entry)
				if err != nil {
					return nil, err
				}
				cmp, err := db.compareKey(kv.KeySlot, key)
				if err != nil {
					return nil, err
				}
				if cmp == 0 {
					pos := nodePtr + BTreeNodeHeaderSize + int64(i)*SlotLength
					return &SortedSlot{Slot: entry, Position: pos}, nil
				}
				if cmp > 0 {
					return nil, nil
				}
			}
			return nil, nil
		}
		i := 0
		for i+1 < node.Num {
			c, err := db.compareKey(node.Separators[i+1], key)
			if err != nil {
				return nil, err
			}
			if c <= 0 {
				i++
			} else {
				break
			}
		}
		nodePtr = node.Children[i].Value
	}
}

// descend by rank to the leaf entry at the given 0-based index
func (db *Database) sortedGetByIndex(rootPtr, rank int64) (SortedSlot, error) {
	nodePtr := rootPtr
	rem := rank
	for {
		node, err := db.readSortedNode(nodePtr)
		if err != nil {
			return SortedSlot{}, err
		}
		if node.Kind == BTreeKindLeaf {
			pos := nodePtr + BTreeNodeHeaderSize + rem*SlotLength
			return SortedSlot{Slot: node.Entries[rem], Position: pos}, nil
		}
		i := 0
		for i+1 < node.Num && rem >= node.Counts[i] {
			rem -= node.Counts[i]
			i++
		}
		nodePtr = node.Children[i].Value
	}
}

// number of keys strictly less than key (the inverse of getByIndex)
func (db *Database) sortedRank(rootPtr int64, key []byte) (int64, error) {
	nodePtr := rootPtr
	var rank int64
	for {
		node, err := db.readSortedNode(nodePtr)
		if err != nil {
			return 0, err
		}
		if node.Kind == BTreeKindLeaf {
			for i := 0; i < node.Num; i++ {
				kv, err := db.readKvPair(node.Entries[i])
				if err != nil {
					return 0, err
				}
				c, err := db.compareKey(kv.KeySlot, key)
				if err != nil {
					return 0, err
				}
				if c < 0 {
					rank++
				} else {
					break
				}
			}
			return rank, nil
		}
		i := 0
		for i+1 < node.Num {
			c, err := db.compareKey(node.Separators[i+1], key)
			if err != nil {
				return 0, err
			}
			if c <= 0 {
				rank += node.Counts[i]
				i++
			} else {
				break
			}
		}
		nodePtr = node.Children[i].Value
	}
}

// write a byte key as a short_bytes (inline, <=8 bytes, no interior zero) or external
// bytes slot
func (db *Database) writeKey(key []byte) (Slot, error) {
	hasZero := false
	for _, b := range key {
		if b == 0 {
			hasZero = true
			break
		}
	}
	if len(key) <= 8 && !hasZero {
		var value [8]byte
		copy(value[:], key)
		return Slot{Value: int64(binary.BigEndian.Uint64(value[:])), Tag: TagShortBytes}, nil
	}
	pos, err := db.Core.Length()
	if err != nil {
		return Slot{}, err
	}
	if err := db.Core.SeekTo(pos); err != nil {
		return Slot{}, err
	}
	if err := writeLong(db.Core, int64(len(key))); err != nil {
		return Slot{}, err
	}
	if err := db.Core.Write(key); err != nil {
		return Slot{}, err
	}
	return Slot{Value: pos, Tag: TagBytes}, nil
}

// materialize a new leaf entry: write the key bytes and a key-value pair with
// an empty value (the caller fills it in). navigation is by key bytes, so the
// entry has no hash.
func (db *Database) sortedNewEntry(key []byte) (SortedEntry, error) {
	keySlot, err := db.writeKey(key)
	if err != nil {
		return SortedEntry{}, err
	}
	kvSlot, err := db.writeKvPair(TagSortedKVPair, KeyValuePair{ValueSlot: Slot{}, KeySlot: keySlot, Hash: make([]byte, db.Header.HashSize)})
	if err != nil {
		return SortedEntry{}, err
	}
	return SortedEntry{KvSlot: kvSlot, KeySlot: keySlot}, nil
}

// insert key (or locate it for replacement) within the subtree at nodePtr,
// path-copying nodes and maintaining separators + counts. the caller writes the value
// of the returned entry.
func (db *Database) sortedPut(nodePtr int64, key []byte) (SortedInsertResult, error) {
	node, err := db.readSortedNode(nodePtr)
	if err != nil {
		return SortedInsertResult{}, err
	}
	switch node.Kind {
	case BTreeKindLeaf:
		idx := node.Num
		found := false
		for i := 0; i < node.Num; i++ {
			kv, err := db.readKvPair(node.Entries[i])
			if err != nil {
				return SortedInsertResult{}, err
			}
			cmp, err := db.compareKey(kv.KeySlot, key)
			if err != nil {
				return SortedInsertResult{}, err
			}
			if cmp == 0 {
				idx = i
				found = true
				break
			}
			if cmp > 0 {
				idx = i
				break
			}
		}

		if found {
			// replace: return a writable value slot, copy-on-writing the kv_pair if it
			// belongs to a past moment
			leaf := node
			kvSlot := node.Entries[idx]
			if !db.btreeReusable(kvSlot.Value) {
				kv, err := db.readKvPair(kvSlot)
				if err != nil {
					return SortedInsertResult{}, err
				}
				kvSlot, err = db.writeKvPair(kvSlot.Tag, kv)
				if err != nil {
					return SortedInsertResult{}, err
				}
				leaf.Entries[idx] = kvSlot
			}
			ptr, err := db.sortedWriteNode(leaf, nodePtr)
			if err != nil {
				return SortedInsertResult{}, err
			}
			return SortedInsertResult{NodePtr: ptr, Count: int64(node.Num), KvSlot: kvSlot, Added: false}, nil
		}

		// insert a new entry at idx
		entry, err := db.sortedNewEntry(key)
		if err != nil {
			return SortedInsertResult{}, err
		}
		entries := make([]Slot, 0, BTreeSlotCount+1)
		entries = append(entries, node.Entries[:idx]...)
		entries = append(entries, entry.KvSlot)
		entries = append(entries, node.Entries[idx:node.Num]...)
		total := node.Num + 1

		if total <= BTreeSlotCount {
			leaf := SortedNode{Kind: BTreeKindLeaf, Num: total}
			copy(leaf.Entries[:total], entries[:total])
			ptr, err := db.sortedWriteNode(leaf, nodePtr)
			if err != nil {
				return SortedInsertResult{}, err
			}
			return SortedInsertResult{NodePtr: ptr, Count: int64(total), KvSlot: entry.KvSlot, Added: true}, nil
		}

		// overflow: split into two leaves; the new sibling's separator is the key of its
		// first entry
		leftN := BTreeSplitCount
		rightN := total - leftN
		left := SortedNode{Kind: BTreeKindLeaf, Num: leftN}
		copy(left.Entries[:leftN], entries[:leftN])
		right := SortedNode{Kind: BTreeKindLeaf, Num: rightN}
		copy(right.Entries[:rightN], entries[leftN:total])
		sepKv, err := db.readKvPair(entries[leftN])
		if err != nil {
			return SortedInsertResult{}, err
		}
		separator := sepKv.KeySlot
		leftPtr, err := db.sortedWriteNode(left, nodePtr)
		if err != nil {
			return SortedInsertResult{}, err
		}
		rightPtr, err := db.writeSortedNode(right)
		if err != nil {
			return SortedInsertResult{}, err
		}
		return SortedInsertResult{NodePtr: leftPtr, Count: int64(leftN), KvSlot: entry.KvSlot, Added: true, Split: &SortedSplit{NodePtr: rightPtr, Count: int64(rightN), Separator: separator}}, nil
	case BTreeKindBranch:
		i := 0
		for i+1 < node.Num {
			c, err := db.compareKey(node.Separators[i+1], key)
			if err != nil {
				return SortedInsertResult{}, err
			}
			if c <= 0 {
				i++
			} else {
				break
			}
		}
		child, err := db.sortedPut(node.Children[i].Value, key)
		if err != nil {
			return SortedInsertResult{}, err
		}

		children := make([]Slot, node.Num, BTreeSlotCount+1)
		separators := make([]Slot, node.Num, BTreeSlotCount+1)
		counts := make([]int64, node.Num, BTreeSlotCount+1)
		copy(children, node.Children[:node.Num])
		copy(separators, node.Separators[:node.Num])
		copy(counts, node.Counts[:node.Num])
		children[i] = Slot{Value: child.NodePtr, Tag: TagIndex}
		counts[i] = child.Count
		total := node.Num
		if child.Split != nil {
			children = children[:node.Num+1]
			separators = separators[:node.Num+1]
			counts = counts[:node.Num+1]
			for j := node.Num; j > i+1; j-- {
				children[j] = children[j-1]
				separators[j] = separators[j-1]
				counts[j] = counts[j-1]
			}
			children[i+1] = Slot{Value: child.Split.NodePtr, Tag: TagIndex}
			separators[i+1] = child.Split.Separator
			counts[i+1] = child.Split.Count
			total = node.Num + 1
		}

		if total <= BTreeSlotCount {
			branch := SortedNode{Kind: BTreeKindBranch, Num: total}
			copy(branch.Children[:total], children[:total])
			copy(branch.Separators[:total], separators[:total])
			copy(branch.Counts[:total], counts[:total])
			ptr, err := db.sortedWriteNode(branch, nodePtr)
			if err != nil {
				return SortedInsertResult{}, err
			}
			return SortedInsertResult{NodePtr: ptr, Count: branch.SubtreeCount(), KvSlot: child.KvSlot, Added: child.Added}, nil
		}

		// overflow: split into two branches; the new sibling's separator is the smallest
		// key of its first child (separators[leftN] of the combined)
		leftN := BTreeSplitCount
		rightN := total - leftN
		left := SortedNode{Kind: BTreeKindBranch, Num: leftN}
		copy(left.Children[:leftN], children[:leftN])
		copy(left.Separators[:leftN], separators[:leftN])
		copy(left.Counts[:leftN], counts[:leftN])
		right := SortedNode{Kind: BTreeKindBranch, Num: rightN}
		copy(right.Children[:rightN], children[leftN:total])
		copy(right.Separators[:rightN], separators[leftN:total])
		copy(right.Counts[:rightN], counts[leftN:total])
		separator := separators[leftN]
		leftPtr, err := db.sortedWriteNode(left, nodePtr)
		if err != nil {
			return SortedInsertResult{}, err
		}
		rightPtr, err := db.writeSortedNode(right)
		if err != nil {
			return SortedInsertResult{}, err
		}
		return SortedInsertResult{NodePtr: leftPtr, Count: left.SubtreeCount(), KvSlot: child.KvSlot, Added: child.Added, Split: &SortedSplit{NodePtr: rightPtr, Count: right.SubtreeCount(), Separator: separator}}, nil
	}
	return SortedInsertResult{}, ErrUnreachable
}

// remove key from the subtree at nodePtr, path-copying nodes and decrementing counts.
// an emptied leaf is left in place (see SortedRemoveResult).
func (db *Database) sortedRemove(nodePtr int64, key []byte) (SortedRemoveResult, error) {
	node, err := db.readSortedNode(nodePtr)
	if err != nil {
		return SortedRemoveResult{}, err
	}
	switch node.Kind {
	case BTreeKindLeaf:
		idx := node.Num
		found := false
		for i := 0; i < node.Num; i++ {
			kv, err := db.readKvPair(node.Entries[i])
			if err != nil {
				return SortedRemoveResult{}, err
			}
			cmp, err := db.compareKey(kv.KeySlot, key)
			if err != nil {
				return SortedRemoveResult{}, err
			}
			if cmp == 0 {
				idx = i
				found = true
				break
			}
			if cmp > 0 {
				break
			}
		}
		if !found {
			return SortedRemoveResult{NodePtr: nodePtr, Found: false}, nil
		}
		leaf := SortedNode{Kind: BTreeKindLeaf, Num: node.Num - 1}
		copy(leaf.Entries[:idx], node.Entries[:idx])
		copy(leaf.Entries[idx:node.Num-1], node.Entries[idx+1:node.Num])
		ptr, err := db.sortedWriteNode(leaf, nodePtr)
		if err != nil {
			return SortedRemoveResult{}, err
		}
		return SortedRemoveResult{NodePtr: ptr, Found: true}, nil
	case BTreeKindBranch:
		i := 0
		for i+1 < node.Num {
			c, err := db.compareKey(node.Separators[i+1], key)
			if err != nil {
				return SortedRemoveResult{}, err
			}
			if c <= 0 {
				i++
			} else {
				break
			}
		}
		child, err := db.sortedRemove(node.Children[i].Value, key)
		if err != nil {
			return SortedRemoveResult{}, err
		}
		if !child.Found {
			return SortedRemoveResult{NodePtr: nodePtr, Found: false}, nil
		}
		branch := node
		branch.Children[i] = Slot{Value: child.NodePtr, Tag: TagIndex}
		branch.Counts[i] -= 1
		ptr, err := db.sortedWriteNode(branch, nodePtr)
		if err != nil {
			return SortedRemoveResult{}, err
		}
		return SortedRemoveResult{NodePtr: ptr, Found: true}, nil
	}
	return SortedRemoveResult{}, ErrUnreachable
}

func (db *Database) sortedGrowRoot(result SortedInsertResult) (int64, error) {
	if result.Split != nil {
		root := SortedNode{Kind: BTreeKindBranch, Num: 2}
		root.Children[0] = Slot{Value: result.NodePtr, Tag: TagIndex}
		root.Children[1] = Slot{Value: result.Split.NodePtr, Tag: TagIndex}
		root.Separators[1] = result.Split.Separator // Separators[0] is an unused sentinel
		root.Counts[0] = result.Count
		root.Counts[1] = result.Split.Count
		return db.writeSortedNode(root)
	}
	return result.NodePtr, nil
}

// turn a located/inserted entry into the slot for the requested target. only
// the value is writeable (that is how put works); the key and the entry
// pointer are immutable, so they are returned with no writeable position.
func (db *Database) sortedTargetSlot(kvSlot Slot, target SortedMapGetTarget) (SlotPointer, error) {
	kv, err := db.readKvPair(kvSlot)
	if err != nil {
		return SlotPointer{}, err
	}
	switch target.(type) {
	case SortedMapGetKVPair:
		return SlotPointer{Position: nil, Slot: kvSlot}, nil
	case SortedMapGetKey:
		return SlotPointer{Position: nil, Slot: kv.KeySlot}, nil
	case SortedMapGetValue:
		pos := kvSlot.Value + db.kvKeyOffset(kvSlot.Tag) + SlotLength
		return SlotPointer{Position: &pos, Slot: kv.ValueSlot}, nil
	default:
		return SlotPointer{}, ErrUnexpectedTag
	}
}

// Compaction helpers

type objectPopulator func(sourceOffset, targetOffset int64) error

type compactor struct {
	sourceCore Core
	targetCore Core
	hashSize   uint16
	offsetMap  OffsetMap
}

func (c *compactor) reserveBlock(size int) (int64, error) {
	offset, err := c.targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := c.targetCore.SeekTo(offset); err != nil {
		return 0, err
	}
	if err := c.targetCore.Write(make([]byte, size)); err != nil {
		return 0, err
	}
	return offset, nil
}

func (c *compactor) visitObject(sourceOffset int64, size int, populate objectPopulator) (int64, error) {
	if targetOffset, ok, err := c.offsetMap.Get(sourceOffset); err != nil {
		return 0, err
	} else if ok {
		return targetOffset, nil
	}

	targetOffset, err := c.reserveBlock(size)
	if err != nil {
		return 0, err
	}
	if err := c.offsetMap.Put(sourceOffset, targetOffset); err != nil {
		return 0, err
	}
	if err := populate(sourceOffset, targetOffset); err != nil {
		return 0, err
	}
	return targetOffset, nil
}

func (c *compactor) remapSlot(slot Slot) (Slot, error) {
	var newOffset int64
	var err error
	switch slot.Tag {
	case TagNone, TagUint, TagInt, TagFloat, TagShortBytes:
		return slot, nil
	case TagBytes:
		newOffset, err = c.remapBytes(slot)
	case TagIndex:
		newOffset, err = c.visitObject(slot.Value, IndexBlockSize, c.populateIndex)
	case TagArrayList:
		newOffset, err = c.visitObject(slot.Value, ArrayListHeaderLength, c.populateArrayList)
	case TagLinkedArrayList:
		newOffset, err = c.visitObject(slot.Value, BTreeHeaderLength, c.populateBTree)
	case TagHashMap, TagHashSet:
		newOffset, err = c.visitObject(slot.Value, IndexBlockSize, c.populateHashMapOrSet)
	case TagCountedHashMap, TagCountedHashSet:
		newOffset, err = c.visitObject(slot.Value, IndexBlockSize+8, c.populateCountedHashMapOrSet)
	case TagKVPair:
		newOffset, err = c.visitObject(slot.Value, KeyValuePairLength(int(c.hashSize)), c.kvPairPopulator(TagKVPair))
	case TagSortedKVPair:
		newOffset, err = c.visitObject(slot.Value, SortedKeyValuePairLength, c.kvPairPopulator(TagSortedKVPair))
	case TagSortedMap, TagSortedSet:
		newOffset, err = c.visitObject(slot.Value, BTreeHeaderLength, c.populateSortedMap)
	default:
		return Slot{}, ErrUnexpectedTag
	}
	if err != nil {
		return Slot{}, err
	}
	return Slot{Value: newOffset, Tag: slot.Tag, Full: slot.Full}, nil
}

func (c *compactor) remapBytes(slot Slot) (int64, error) {
	if targetOffset, ok, err := c.offsetMap.Get(slot.Value); err != nil {
		return 0, err
	} else if ok {
		return targetOffset, nil
	}

	if err := c.sourceCore.SeekTo(slot.Value); err != nil {
		return 0, err
	}
	length, err := readLong(c.sourceCore)
	if err != nil {
		return 0, err
	}

	formatTagSize := int64(0)
	if slot.Full {
		formatTagSize = 2
	}
	totalPayload := length + formatTagSize

	newOffset, err := c.targetCore.Length()
	if err != nil {
		return 0, err
	}
	if err := c.targetCore.SeekTo(newOffset); err != nil {
		return 0, err
	}
	if err := writeLong(c.targetCore, length); err != nil {
		return 0, err
	}

	remaining := totalPayload
	for remaining > 0 {
		chunk := int64(4096)
		if remaining < chunk {
			chunk = remaining
		}
		buf := make([]byte, chunk)
		if err := c.sourceCore.Read(buf); err != nil {
			return 0, err
		}
		if err := c.targetCore.Write(buf); err != nil {
			return 0, err
		}
		remaining -= chunk
	}

	if err := c.offsetMap.Put(slot.Value, newOffset); err != nil {
		return 0, err
	}
	return newOffset, nil
}

func (c *compactor) populateIndex(sourceOffset, targetOffset int64) error {
	if err := c.sourceCore.SeekTo(sourceOffset); err != nil {
		return err
	}
	blockBytes := make([]byte, IndexBlockSize)
	if err := c.sourceCore.Read(blockBytes); err != nil {
		return err
	}

	var remappedSlots [SlotCount]Slot
	for i := 0; i < SlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], blockBytes[i*SlotLength:(i+1)*SlotLength])
		childSlot := SlotFromBytes(sb)
		remapped, err := c.remapSlot(childSlot)
		if err != nil {
			return err
		}
		remappedSlots[i] = remapped
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	for _, s := range remappedSlots {
		b := s.ToBytes()
		if err := c.targetCore.Write(b[:]); err != nil {
			return err
		}
	}

	return nil
}

func (c *compactor) remapArrayListIndex(sourceOffset, size int64, shift byte) (int64, error) {
	childSize := int64(1) << (shift * BitCount)

	// full blocks can use the normal cache. partial blocks may
	// be shared by lists with different sizes, so copy them
	// separately and leave the slots beyond the size empty.
	if childSize <= math.MaxInt64/SlotCount && size == childSize*SlotCount {
		slot, err := c.remapSlot(Slot{Value: sourceOffset, Tag: TagIndex})
		return slot.Value, err
	}

	targetOffset, err := c.reserveBlock(IndexBlockSize)
	if err != nil {
		return 0, err
	}
	if err := c.sourceCore.SeekTo(sourceOffset); err != nil {
		return 0, err
	}
	var blockBytes [IndexBlockSize]byte
	if err := c.sourceCore.Read(blockBytes[:]); err != nil {
		return 0, err
	}
	var remappedBlock [IndexBlockSize]byte
	remaining := size
	for i := 0; i < SlotCount && remaining > 0; i++ {
		var slotBytes [SlotLength]byte
		copy(slotBytes[:], blockBytes[i*SlotLength:(i+1)*SlotLength])
		childSlot := SlotFromBytes(slotBytes)
		count := min(remaining, childSize)
		remappedSlot := childSlot
		if shift == 0 {
			remappedSlot, err = c.remapSlot(childSlot)
		} else {
			if childSlot.Tag != TagIndex {
				return 0, ErrUnexpectedTag
			}
			remappedSlot.Value, err = c.remapArrayListIndex(childSlot.Value, count, shift-1)
		}
		if err != nil {
			return 0, err
		}
		b := remappedSlot.ToBytes()
		copy(remappedBlock[i*SlotLength:(i+1)*SlotLength], b[:])
		remaining -= count
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return 0, err
	}
	if err := c.targetCore.Write(remappedBlock[:]); err != nil {
		return 0, err
	}
	return targetOffset, nil
}

func (c *compactor) populateArrayList(sourceOffset, targetOffset int64) error {
	if err := c.sourceCore.SeekTo(sourceOffset); err != nil {
		return err
	}
	var headerBytes [ArrayListHeaderLength]byte
	if err := c.sourceCore.Read(headerBytes[:]); err != nil {
		return err
	}
	header, err := ArrayListHeaderFromBytes(headerBytes[:])
	if err != nil {
		return err
	}

	var shift byte
	if header.Size > SlotCount {
		shift = byte(math.Log(float64(header.Size-1)) / math.Log(float64(SlotCount)))
	}
	remappedIndex, err := c.remapArrayListIndex(header.Ptr, header.Size, shift)
	if err != nil {
		return err
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	newHeader := ArrayListHeader{Ptr: remappedIndex, Size: header.Size}
	nhb := newHeader.ToBytes()
	if err := c.targetCore.Write(nhb[:]); err != nil {
		return err
	}

	return nil
}

func (c *compactor) populateBTree(sourceOffset, targetOffset int64) error {
	if err := c.sourceCore.SeekTo(sourceOffset); err != nil {
		return err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := c.sourceCore.Read(headerBytes[:]); err != nil {
		return err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return err
	}

	remappedRoot, err := c.remapBTreeNode(header.RootPtr)
	if err != nil {
		return err
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	newHeader := BTreeHeader{RootPtr: remappedRoot, Size: header.Size}
	nhb := newHeader.ToBytes()
	if err := c.targetCore.Write(nhb[:]); err != nil {
		return err
	}

	return nil
}

func (c *compactor) remapBTreeNode(nodeOffset int64) (int64, error) {
	if mapped, ok, err := c.offsetMap.Get(nodeOffset); err != nil {
		return 0, err
	} else if ok {
		return mapped, nil
	}

	if err := c.sourceCore.SeekTo(nodeOffset); err != nil {
		return 0, err
	}
	var nodeHeader [BTreeNodeHeaderSize]byte
	if err := c.sourceCore.Read(nodeHeader[:]); err != nil {
		return 0, err
	}
	kindInt := nodeHeader[0]
	if kindInt > byte(BTreeKindBranch) {
		return 0, ErrInvalidBTreeNodeKind
	}
	kind := BTreeNodeKind(kindInt)
	num := nodeHeader[1]
	if num > BTreeSlotCount {
		return 0, ErrInvalidBTreeNode
	}

	switch kind {
	case BTreeKindLeaf:
		return c.visitObject(nodeOffset, BTreeLeafBlockSize, func(sourceOffset, targetOffset int64) error {
			return c.populateBTreeLeaf(sourceOffset, targetOffset, kindInt, num)
		})
	case BTreeKindBranch:
		return c.visitObject(nodeOffset, BTreeBranchBlockSize, func(sourceOffset, targetOffset int64) error {
			return c.populateBTreeBranch(sourceOffset, targetOffset, kindInt, num)
		})
	}
	return 0, ErrUnreachable
}

func (c *compactor) populateBTreeLeaf(sourceOffset, targetOffset int64, kind, num byte) error {
	if err := c.sourceCore.SeekTo(sourceOffset + BTreeNodeHeaderSize); err != nil {
		return err
	}
	body := make([]byte, SlotLength*BTreeSlotCount)
	if err := c.sourceCore.Read(body); err != nil {
		return err
	}

	var slots [BTreeSlotCount]Slot
	for i := 0; i < BTreeSlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
		remapped, err := c.remapSlot(SlotFromBytes(sb))
		if err != nil {
			return err
		}
		slots[i] = remapped
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	if err := c.targetCore.Write([]byte{kind, num}); err != nil {
		return err
	}
	for _, slot := range slots {
		b := slot.ToBytes()
		if err := c.targetCore.Write(b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (c *compactor) populateBTreeBranch(sourceOffset, targetOffset int64, kind, num byte) error {
	if err := c.sourceCore.SeekTo(sourceOffset + BTreeNodeHeaderSize); err != nil {
		return err
	}
	body := make([]byte, (SlotLength+8)*BTreeSlotCount)
	if err := c.sourceCore.Read(body); err != nil {
		return err
	}

	var children [BTreeSlotCount]Slot
	for i := 0; i < BTreeSlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
		child := SlotFromBytes(sb)
		// non-index children are copied verbatim, so validate the tag
		// here; slots that go through remapSlot are validated there
		if err := child.Tag.validate(); err != nil {
			return err
		}
		if child.Tag == TagIndex {
			remappedPtr, err := c.remapBTreeNode(child.Value)
			if err != nil {
				return err
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

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	if err := c.targetCore.Write([]byte{kind, num}); err != nil {
		return err
	}
	for _, child := range children {
		b := child.ToBytes()
		if err := c.targetCore.Write(b[:]); err != nil {
			return err
		}
	}
	for _, count := range counts {
		var cb [8]byte
		binary.BigEndian.PutUint64(cb[:], uint64(count))
		if err := c.targetCore.Write(cb[:]); err != nil {
			return err
		}
	}
	return nil
}

func (c *compactor) populateSortedMap(sourceOffset, targetOffset int64) error {
	if err := c.sourceCore.SeekTo(sourceOffset); err != nil {
		return err
	}
	var headerBytes [BTreeHeaderLength]byte
	if err := c.sourceCore.Read(headerBytes[:]); err != nil {
		return err
	}
	header, err := BTreeHeaderFromBytes(headerBytes[:])
	if err != nil {
		return err
	}

	remappedRoot, err := c.remapSortedMapNode(header.RootPtr)
	if err != nil {
		return err
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	newHeader := BTreeHeader{RootPtr: remappedRoot, Size: header.Size}
	nhb := newHeader.ToBytes()
	if err := c.targetCore.Write(nhb[:]); err != nil {
		return err
	}

	return nil
}

func (c *compactor) remapSortedMapNode(nodeOffset int64) (int64, error) {
	if mapped, ok, err := c.offsetMap.Get(nodeOffset); err != nil {
		return 0, err
	} else if ok {
		return mapped, nil
	}

	if err := c.sourceCore.SeekTo(nodeOffset); err != nil {
		return 0, err
	}
	var nodeHeader [BTreeNodeHeaderSize]byte
	if err := c.sourceCore.Read(nodeHeader[:]); err != nil {
		return 0, err
	}
	kindInt := nodeHeader[0]
	if kindInt > byte(BTreeKindBranch) {
		return 0, ErrInvalidBTreeNodeKind
	}
	kind := BTreeNodeKind(kindInt)
	num := nodeHeader[1]
	if num > BTreeSlotCount {
		return 0, ErrInvalidBTreeNode
	}

	switch kind {
	case BTreeKindLeaf:
		return c.visitObject(nodeOffset, SortedLeafBlockSize, func(sourceOffset, targetOffset int64) error {
			return c.populateSortedMapLeaf(sourceOffset, targetOffset, kindInt, num)
		})
	case BTreeKindBranch:
		return c.visitObject(nodeOffset, SortedBranchBlockSize, func(sourceOffset, targetOffset int64) error {
			return c.populateSortedMapBranch(sourceOffset, targetOffset, kindInt, num)
		})
	}
	return 0, ErrUnreachable
}

func (c *compactor) populateSortedMapLeaf(sourceOffset, targetOffset int64, kind, num byte) error {
	if err := c.sourceCore.SeekTo(sourceOffset + BTreeNodeHeaderSize); err != nil {
		return err
	}
	body := make([]byte, SlotLength*BTreeSlotCount)
	if err := c.sourceCore.Read(body); err != nil {
		return err
	}

	var entries [BTreeSlotCount]Slot
	for i := 0; i < BTreeSlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
		remapped, err := c.remapSlot(SlotFromBytes(sb))
		if err != nil {
			return err
		}
		entries[i] = remapped
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	if err := c.targetCore.Write([]byte{kind, num}); err != nil {
		return err
	}
	for _, entry := range entries {
		b := entry.ToBytes()
		if err := c.targetCore.Write(b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (c *compactor) populateSortedMapBranch(sourceOffset, targetOffset int64, kind, num byte) error {
	if err := c.sourceCore.SeekTo(sourceOffset + BTreeNodeHeaderSize); err != nil {
		return err
	}
	body := make([]byte, (SlotLength*2+8)*BTreeSlotCount)
	if err := c.sourceCore.Read(body); err != nil {
		return err
	}

	var children [BTreeSlotCount]Slot
	for i := 0; i < BTreeSlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], body[i*SlotLength:i*SlotLength+SlotLength])
		child := SlotFromBytes(sb)
		// non-index children are copied verbatim, so validate the tag
		// here; slots that go through remapSlot are validated there
		if err := child.Tag.validate(); err != nil {
			return err
		}
		if child.Tag == TagIndex {
			remappedPtr, err := c.remapSortedMapNode(child.Value)
			if err != nil {
				return err
			}
			children[i] = Slot{Value: remappedPtr, Tag: TagIndex, Full: child.Full}
		} else {
			children[i] = child
		}
	}
	separatorOffset := SlotLength * BTreeSlotCount
	var separators [BTreeSlotCount]Slot
	for i := 0; i < BTreeSlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], body[separatorOffset+i*SlotLength:separatorOffset+i*SlotLength+SlotLength])
		remapped, err := c.remapSlot(SlotFromBytes(sb))
		if err != nil {
			return err
		}
		separators[i] = remapped
	}
	countsOffset := SlotLength * 2 * BTreeSlotCount
	var counts [BTreeSlotCount]int64
	for i := 0; i < BTreeSlotCount; i++ {
		counts[i] = int64(binary.BigEndian.Uint64(body[countsOffset+i*8 : countsOffset+i*8+8]))
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	if err := c.targetCore.Write([]byte{kind, num}); err != nil {
		return err
	}
	for _, child := range children {
		b := child.ToBytes()
		if err := c.targetCore.Write(b[:]); err != nil {
			return err
		}
	}
	for _, separator := range separators {
		b := separator.ToBytes()
		if err := c.targetCore.Write(b[:]); err != nil {
			return err
		}
	}
	for _, count := range counts {
		var cb [8]byte
		binary.BigEndian.PutUint64(cb[:], uint64(count))
		if err := c.targetCore.Write(cb[:]); err != nil {
			return err
		}
	}
	return nil
}

func (c *compactor) populateHashMapOrSet(sourceOffset, targetOffset int64) error {
	return c.populateHashMapOrSetCounted(sourceOffset, targetOffset, false)
}

func (c *compactor) populateCountedHashMapOrSet(sourceOffset, targetOffset int64) error {
	return c.populateHashMapOrSetCounted(sourceOffset, targetOffset, true)
}

func (c *compactor) populateHashMapOrSetCounted(sourceOffset, targetOffset int64, counted bool) error {
	if err := c.sourceCore.SeekTo(sourceOffset); err != nil {
		return err
	}

	var countValue int64 = -1
	if counted {
		var err error
		countValue, err = readLong(c.sourceCore)
		if err != nil {
			return err
		}
	}

	blockBytes := make([]byte, IndexBlockSize)
	if err := c.sourceCore.Read(blockBytes); err != nil {
		return err
	}

	var remappedSlots [SlotCount]Slot
	for i := 0; i < SlotCount; i++ {
		var sb [SlotLength]byte
		copy(sb[:], blockBytes[i*SlotLength:(i+1)*SlotLength])
		childSlot := SlotFromBytes(sb)
		remapped, err := c.remapSlot(childSlot)
		if err != nil {
			return err
		}
		remappedSlots[i] = remapped
	}

	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	if counted {
		if err := writeLong(c.targetCore, countValue); err != nil {
			return err
		}
	}
	for _, s := range remappedSlots {
		b := s.ToBytes()
		if err := c.targetCore.Write(b[:]); err != nil {
			return err
		}
	}

	return nil
}

func (c *compactor) kvPairPopulator(tag Tag) objectPopulator {
	return func(sourceOffset, targetOffset int64) error {
		return c.populateKvPair(sourceOffset, targetOffset, tag)
	}
}

func (c *compactor) populateKvPair(sourceOffset, targetOffset int64, tag Tag) error {
	if err := c.sourceCore.SeekTo(sourceOffset); err != nil {
		return err
	}
	kvPair, err := decodeKvPair(c.sourceCore, tag, int(c.hashSize))
	if err != nil {
		return err
	}

	// remap KeySlot and ValueSlot, and write the pair in the same form
	kvPair.KeySlot, err = c.remapSlot(kvPair.KeySlot)
	if err != nil {
		return err
	}
	kvPair.ValueSlot, err = c.remapSlot(kvPair.ValueSlot)
	if err != nil {
		return err
	}
	if err := c.targetCore.SeekTo(targetOffset); err != nil {
		return err
	}
	return encodeKvPair(c.targetCore, tag, kvPair)
}
