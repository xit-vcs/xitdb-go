package xitdb

import "os"

type CoreBufferedFile struct {
	file       *os.File
	memory     *CoreMemory
	bufferSize int
	filePos    int64
	memoryPos  int64
}

func NewCoreBufferedFile(f *os.File) *CoreBufferedFile {
	return NewCoreBufferedFileWithSize(f, 8*1024*1024)
}

func NewCoreBufferedFileWithSize(f *os.File, bufferSize int) *CoreBufferedFile {
	return &CoreBufferedFile{
		file:       f,
		memory:     NewCoreMemory(),
		bufferSize: bufferSize,
		filePos:    0,
		memoryPos:  0,
	}
}

func (c *CoreBufferedFile) memorySize() int64 {
	return int64(len(c.memory.buf))
}

func (c *CoreBufferedFile) Read(p []byte) error {
	pos := 0

	// read from disk -- before the in-memory buffer
	if c.filePos < c.memoryPos {
		sizeBeforeMem := min(len(p), int(c.memoryPos-c.filePos))
		if _, err := c.file.Seek(c.filePos, 0); err != nil {
			return err
		}
		if _, err := c.file.Read(p[:sizeBeforeMem]); err != nil {
			return err
		}
		pos += sizeBeforeMem
		c.filePos += int64(sizeBeforeMem)
	}

	if pos == len(p) {
		return nil
	}

	// read from the in-memory buffer
	if c.filePos >= c.memoryPos && c.filePos < c.memoryPos+c.memorySize() {
		memPos := int(c.filePos - c.memoryPos)
		sizeInMem := min(int(c.memorySize())-memPos, len(p)-pos)
		copy(p[pos:pos+sizeInMem], c.memory.buf[memPos:memPos+sizeInMem])
		pos += sizeInMem
		c.filePos += int64(sizeInMem)
	}

	if pos == len(p) {
		return nil
	}

	// read from disk -- after the in-memory buffer
	if c.filePos >= c.memoryPos+c.memorySize() {
		sizeAfterMem := len(p) - pos
		if _, err := c.file.Seek(c.filePos, 0); err != nil {
			return err
		}
		if _, err := c.file.Read(p[pos : pos+sizeAfterMem]); err != nil {
			return err
		}
		c.filePos += int64(sizeAfterMem)
	}

	return nil
}

func (c *CoreBufferedFile) Write(p []byte) error {
	if c.memorySize()+int64(len(p)) > int64(c.bufferSize) {
		if err := c.Flush(); err != nil {
			return err
		}
	}

	if c.filePos >= c.memoryPos && c.filePos <= c.memoryPos+c.memorySize() {
		if err := c.memory.SeekTo(c.filePos - c.memoryPos); err != nil {
			return err
		}
		if err := c.memory.Write(p); err != nil {
			return err
		}
	} else {
		if _, err := c.file.Seek(c.filePos, 0); err != nil {
			return err
		}
		if _, err := c.file.Write(p); err != nil {
			return err
		}
	}

	c.filePos += int64(len(p))
	return nil
}

func (c *CoreBufferedFile) Length() (int64, error) {
	info, err := c.file.Stat()
	if err != nil {
		return 0, err
	}
	fileLen := info.Size()
	memLen := c.memoryPos + c.memorySize()
	if memLen > fileLen {
		return memLen, nil
	}
	return fileLen, nil
}

func (c *CoreBufferedFile) SeekTo(pos int64) error {
	// flush if we are going past the end of the in-memory buffer
	if pos > c.memoryPos+c.memorySize() {
		if err := c.Flush(); err != nil {
			return err
		}
	}

	c.filePos = pos

	// if the buffer is empty, set its position to this offset as well
	if c.memorySize() == 0 {
		c.memoryPos = pos
	}

	return nil
}

func (c *CoreBufferedFile) Position() (int64, error) {
	return c.filePos, nil
}

func (c *CoreBufferedFile) SetLength(length int64) error {
	if err := c.Flush(); err != nil {
		return err
	}
	if err := c.file.Truncate(length); err != nil {
		return err
	}
	if length < c.filePos {
		c.filePos = length
	}
	return nil
}

func (c *CoreBufferedFile) Flush() error {
	if c.memorySize() > 0 {
		if _, err := c.file.Seek(c.memoryPos, 0); err != nil {
			return err
		}
		if _, err := c.file.Write(c.memory.buf); err != nil {
			return err
		}
		c.memoryPos = 0
		c.memory.buf = c.memory.buf[:0]
		c.memory.pos = 0
	}
	return nil
}

func (c *CoreBufferedFile) Sync() error {
	if err := c.Flush(); err != nil {
		return err
	}
	return c.file.Sync()
}

func (c *CoreBufferedFile) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}
	return c.file.Close()
}
