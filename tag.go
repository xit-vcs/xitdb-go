package xitdb

type Tag byte

const (
	TagNone             Tag = iota
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
)
