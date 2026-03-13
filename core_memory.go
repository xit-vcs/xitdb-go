package xitdb

import "fmt"

type CoreMemory struct {
	buf []byte
	pos int64
}

func NewCoreMemory() *CoreMemory {
	return &CoreMemory{}
}

func (m *CoreMemory) Read(p []byte) error {
	if int(m.pos)+len(p) > len(m.buf) {
		return fmt.Errorf("read beyond end of memory: pos=%d, len=%d, size=%d", m.pos, len(p), len(m.buf))
	}
	copy(p, m.buf[m.pos:m.pos+int64(len(p))])
	m.pos += int64(len(p))
	return nil
}

func (m *CoreMemory) Write(p []byte) error {
	pos := int(m.pos)
	if pos < len(m.buf) {
		bytesBeforeEnd := min(len(p), len(m.buf)-pos)
		copy(m.buf[pos:pos+bytesBeforeEnd], p[:bytesBeforeEnd])
		if bytesBeforeEnd < len(p) {
			m.buf = append(m.buf, p[bytesBeforeEnd:]...)
		}
	} else {
		// pad with zeros if position is beyond current length
		if pos > len(m.buf) {
			m.buf = append(m.buf, make([]byte, pos-len(m.buf))...)
		}
		m.buf = append(m.buf, p...)
	}
	m.pos = int64(pos + len(p))
	return nil
}

func (m *CoreMemory) Length() (int64, error) {
	return int64(len(m.buf)), nil
}

func (m *CoreMemory) SeekTo(pos int64) error {
	if pos > int64(len(m.buf)) {
		m.pos = int64(len(m.buf))
	} else {
		m.pos = pos
	}
	return nil
}

func (m *CoreMemory) Position() (int64, error) {
	return m.pos, nil
}

func (m *CoreMemory) SetLength(length int64) error {
	if length == 0 {
		m.buf = m.buf[:0]
		m.pos = 0
		return nil
	}
	if length > int64(len(m.buf)) {
		return fmt.Errorf("cannot extend memory length")
	}
	origPos := m.pos
	m.buf = m.buf[:length]
	if origPos > length {
		m.pos = length
	}
	return nil
}

func (m *CoreMemory) Flush() error {
	return nil
}

func (m *CoreMemory) Sync() error {
	return nil
}
