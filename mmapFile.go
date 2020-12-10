package dbf

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/exp/mmap"
)

type mmapFile struct {
	file     *os.File
	readerAt *mmap.ReaderAt
	offset   int64
}

func newMmapFile(f *os.File) *mmapFile {
	ra, err := mmap.Open(f.Name())
	if err != nil {
		panic(err)
	}
	return &mmapFile{
		file:     f,
		readerAt: ra,
	}
}
func (f *mmapFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}
func (f *mmapFile) Name() string {
	return f.file.Name()
}
func (f *mmapFile) Close() error {
	f.file.Close()
	return f.readerAt.Close()
}
func (f *mmapFile) Read(b []byte) (int, error) {
	n, err := f.ReadAt(b, f.offset)
	if err != nil {
		return n, err
	}
	f.offset += int64(n)
	return n, nil
}
func (f *mmapFile) ReadAt(b []byte, offset int64) (int, error) {
	return f.readerAt.ReadAt(b, offset)
}
func (f *mmapFile) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart {
		f.offset = offset
	} else if whence == io.SeekCurrent {
		f.offset += offset
	} else if whence == io.SeekEnd {
		f.offset = int64(f.readerAt.Len() - int(offset))
	} else {
		return f.offset, fmt.Errorf("Invalid parameter 'whence'")
	}
	if f.offset < 0 {
		return f.offset, fmt.Errorf("Can not seek beyond BOF")
	} else if f.offset >= int64(f.readerAt.Len()) {
		return f.offset, fmt.Errorf("Can not seek beyond EOF")
	}
	return f.offset, nil
}
