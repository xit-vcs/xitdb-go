package xitdb

import (
	"io"
	"os"
)

type CoreBufferedFile struct {
	file       *os.File
	memory     *CoreMemory
	bufferSize int
	filePos    int64
	memoryPos  int64
	// the file's length, cached so that Length doesn't need to ask the OS
	// every time data is allocated. another process may write to the file
	// whenever this one isn't, so it is only set once we begin writing, and
	// it is cleared when the writes are flushed.
	fileLen *int64
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
		if _, err := io.ReadFull(c.file, p[:sizeBeforeMem]); err != nil {
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
		if _, err := io.ReadFull(c.file, p[pos:pos+sizeAfterMem]); err != nil {
			return err
		}
		c.filePos += int64(sizeAfterMem)
	}

	return nil
}

func (c *CoreBufferedFile) Write(p []byte) error {
	n := int64(len(p))
	if n == 0 {
		return nil
	}

	// the in-memory buffer is a single contiguous window of the file
	// starting at memoryPos. start a new window at this position if
	// the buffer is empty, the write is past the end of the window,
	// or the write would grow the window beyond the max size.
	if c.memorySize() == 0 ||
		c.filePos > c.memoryPos+c.memorySize() ||
		(c.filePos >= c.memoryPos && c.filePos-c.memoryPos+n > int64(c.bufferSize)) {
		if err := c.Flush(); err != nil {
			return err
		}
		c.memoryPos = c.filePos
	}

	if c.fileLen == nil {
		fileLen, err := c.statLength()
		if err != nil {
			return err
		}
		c.fileLen = &fileLen
	}

	if c.filePos >= c.memoryPos && c.filePos-c.memoryPos+n <= int64(c.bufferSize) {
		// write to the in-memory buffer
		if err := c.memory.SeekTo(c.filePos - c.memoryPos); err != nil {
			return err
		}
		if err := c.memory.Write(p); err != nil {
			return err
		}
	} else {
		// a direct disk write that overlaps the buffered region would be
		// clobbered by a later flush of stale buffer bytes, so flush first
		if c.filePos < c.memoryPos+c.memorySize() && c.filePos+n > c.memoryPos {
			if err := c.Flush(); err != nil {
				return err
			}
		}
		if err := c.writeToFile(c.filePos, p); err != nil {
			return err
		}
	}

	c.filePos += n
	return nil
}

func (c *CoreBufferedFile) statLength() (int64, error) {
	info, err := c.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (c *CoreBufferedFile) Length() (int64, error) {
	var fileLen int64
	if c.fileLen != nil {
		fileLen = *c.fileLen
	} else {
		var err error
		fileLen, err = c.statLength()
		if err != nil {
			return 0, err
		}
	}
	bufferSize := c.memorySize()
	// a failed allocation or a rollback can leave an empty
	// buffer positioned beyond the file's end.
	if bufferSize == 0 {
		return fileLen, nil
	}
	return max(c.memoryPos+bufferSize, fileLen), nil
}

func (c *CoreBufferedFile) SeekTo(pos int64) error {
	c.filePos = pos
	return nil
}

func (c *CoreBufferedFile) Position() (int64, error) {
	return c.filePos, nil
}

func (c *CoreBufferedFile) SetLength(length int64) error {
	// discard buffered bytes past the new end rather than flushing them.
	// a rollback must not depend on writing the data it is throwing away,
	// because that write may be what failed (e.g. the disk is full).
	if length <= c.memoryPos {
		if err := c.memory.SetLength(0); err != nil {
			return err
		}
	} else if length < c.memoryPos+c.memorySize() {
		if err := c.memory.SetLength(length - c.memoryPos); err != nil {
			return err
		}
	}
	c.fileLen = nil
	if err := c.file.Truncate(length); err != nil {
		return err
	}
	if length < c.filePos {
		c.filePos = length
	}
	return nil
}

func (c *CoreBufferedFile) Flush() error {
	c.fileLen = nil
	if c.memorySize() > 0 {
		if err := c.writeToFile(c.memoryPos, c.memory.buf); err != nil {
			return err
		}
		c.memory.buf = c.memory.buf[:0]
		c.memory.pos = 0
	}
	return nil
}

func (c *CoreBufferedFile) writeToFile(pos int64, p []byte) error {
	// if the write fails partway, the file's length is unknown
	fileLen := c.fileLen
	c.fileLen = nil

	if _, err := c.file.Seek(pos, 0); err != nil {
		return err
	}
	if _, err := c.file.Write(p); err != nil {
		return err
	}

	if fileLen != nil {
		newLen := max(*fileLen, pos+int64(len(p)))
		c.fileLen = &newLen
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
