package xitdb

import "encoding/binary"

type Core interface {
	Read(p []byte) error
	Write(p []byte) error
	Length() (int64, error)
	SeekTo(pos int64) error
	Position() (int64, error)
	SetLength(len int64) error
	Flush() error
	Sync() error
}

func readLong(c Core) (int64, error) {
	var buf [8]byte
	if err := c.Read(buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf[:])), nil
}

func writeLong(c Core, v int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return c.Write(buf[:])
}

func readShort(c Core) (int16, error) {
	var buf [2]byte
	if err := c.Read(buf[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(buf[:])), nil
}

func writeShort(c Core, v int16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(v))
	return c.Write(buf[:])
}

func readByte_(c Core) (byte, error) {
	var buf [1]byte
	if err := c.Read(buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func writeByte_(c Core, v byte) error {
	return c.Write([]byte{v})
}

func readInt32(c Core) (int32, error) {
	var buf [4]byte
	if err := c.Read(buf[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf[:])), nil
}

func writeInt32(c Core, v int32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	return c.Write(buf[:])
}
