package xitdb

type Tag byte

const (
	TagNone Tag = iota
	TagIndex
	TagArrayList
	TagLinkedArrayList
	TagHashMap
	TagKVPair
	TagBytes
	TagShortBytes
	TagUint
	TagInt
	TagFloat
	TagHashSet
	TagCountedHashMap
	TagCountedHashSet
	TagSortedMap
	TagSortedSet
	TagSortedKVPair
)

// validate returns ErrUnexpectedTag if the tag is not a known value, so
// corrupted data read from disk yields an error instead of being silently
// propagated.
func (t Tag) validate() error {
	if t > TagSortedKVPair {
		return ErrUnexpectedTag
	}
	return nil
}
