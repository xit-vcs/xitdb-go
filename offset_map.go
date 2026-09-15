package xitdb

// maps source offsets to target offsets during compaction.
// mappings must remain available until reset to preserve sharing and cycles.
// each map requires exclusive use during compaction.
type OffsetMap interface {
	// clears mappings from the previous compaction
	Reset() error

	// returns the target offset and whether the source offset is present
	Get(sourceOffset int64) (targetOffset int64, found bool, err error)

	// records where the source object was copied
	Put(sourceOffset, targetOffset int64) error
}

// stores compaction offsets in memory. initialize with make before use.
type MemoryOffsetMap map[int64]int64

func (m MemoryOffsetMap) Reset() error {
	clear(m)
	return nil
}

func (m MemoryOffsetMap) Get(sourceOffset int64) (int64, bool, error) {
	targetOffset, found := m[sourceOffset]
	return targetOffset, found, nil
}

func (m MemoryOffsetMap) Put(sourceOffset, targetOffset int64) error {
	m[sourceOffset] = targetOffset
	return nil
}
