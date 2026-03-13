package xitdb

import "os"

type CoreFile struct {
	File *os.File
}

func NewCoreFile(f *os.File) *CoreFile {
	return &CoreFile{File: f}
}

func (c *CoreFile) Read(p []byte) error {
	_, err := c.File.Read(p)
	return err
}

func (c *CoreFile) Write(p []byte) error {
	_, err := c.File.Write(p)
	return err
}

func (c *CoreFile) Length() (int64, error) {
	info, err := c.File.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (c *CoreFile) SeekTo(pos int64) error {
	_, err := c.File.Seek(pos, 0)
	return err
}

func (c *CoreFile) Position() (int64, error) {
	return c.File.Seek(0, 1)
}

func (c *CoreFile) SetLength(length int64) error {
	return c.File.Truncate(length)
}

func (c *CoreFile) Flush() error {
	return nil
}

func (c *CoreFile) Sync() error {
	return c.File.Sync()
}
