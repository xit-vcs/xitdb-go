package xitdb

import "encoding/binary"

const SlotLength = 9

type Slot struct {
	Value int64
	Tag   Tag
	Full  bool
}

func (s Slot) WithTag(tag Tag) Slot {
	return Slot{Value: s.Value, Tag: tag, Full: s.Full}
}

func (s Slot) WithFull(full bool) Slot {
	return Slot{Value: s.Value, Tag: s.Tag, Full: full}
}

func (s Slot) Empty() bool {
	return s.Tag == TagNone && !s.Full
}

func (s Slot) ToBytes() [SlotLength]byte {
	var buf [SlotLength]byte
	tagByte := byte(s.Tag)
	if s.Full {
		tagByte |= 0b1000_0000
	}
	buf[0] = tagByte
	binary.BigEndian.PutUint64(buf[1:], uint64(s.Value))
	return buf
}

func SlotFromBytes(b [SlotLength]byte) Slot {
	full := (b[0] & 0b1000_0000) != 0
	tag := Tag(b[0] & 0b0111_1111)
	value := int64(binary.BigEndian.Uint64(b[1:]))
	return Slot{Value: value, Tag: tag, Full: full}
}
