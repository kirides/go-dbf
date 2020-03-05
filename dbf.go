package dbf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/exp/mmap"
	"golang.org/x/text/encoding"
)

const maxBacklinkLenght = 263

// ErrInvalidRecordNumber is returned whenever a provided record number is invalid
var ErrInvalidRecordNumber = errors.New("Invalid record")

type backingTable interface {
	io.ReaderAt
	io.Closer
	Len() int
}

type backingFile struct {
	*os.File
	currentOffset int64
}

func (f *backingFile) Len() int {
	stat, err := f.Stat()
	if err != nil {
		return -1
	}
	return int(stat.Size())
}
func (f *backingFile) ReadAt(buffer []byte, offset int64) (int, error) {
	if f.currentOffset != offset {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return 0, err
		}
		f.currentOffset = offset
	}
	r, err := f.Read(buffer)
	if err != nil {
		return r, err
	}
	f.currentOffset += int64(r)
	return r, nil
}

// Dbf provides methods to access a DBF
type Dbf struct {
	recpointer int32
	dbfFile    backingTable

	memoFile string
	decoder  *encoding.Decoder

	path      string
	header    Header
	fields    []Field
	backlink  string
	nullField *Field
}

var UseMmap = false

// Open opens the specifid DBF
func Open(path string, decoder *encoding.Decoder) (*Dbf, error) {
	var err error
	var dbfFile backingTable
	if UseMmap {
		dbfFile, err = mmap.Open(path)
		if err != nil {
			return nil, err
		}
	} else {
		osF, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		dbfFile = &backingFile{File: osF}
	}

	dbfHeader := Header{}

	buffer := make([]byte, 31, 31)
	dbfFile.ReadAt(buffer, 0)

	if err := binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &dbfHeader); err != nil {
		dbfFile.Close()
		return nil, fmt.Errorf("Could not open table at %q. %w", path, err)
	}

	fields, err := readFields(dbfFile, decoder)
	if err != nil {
		return nil, fmt.Errorf("Could not read field structure. %w", err)
	}

	backlinkBuf := make([]byte, 263)
	if _, err := dbfFile.ReadAt(backlinkBuf, int64(dbfHeader.HeaderSize-maxBacklinkLenght)); err != nil {
		dbfFile.Close()
		return nil, fmt.Errorf("Invalid header size. %w", err)
	}
	backlink := ""
	if backlinkBuf[0] != 0x00 {
		backlink, _ = decoder.String(string(backlinkBuf[:bytes.IndexByte(backlinkBuf, 0x00)]))
	}

	dbf := Dbf{
		dbfFile:  dbfFile,
		header:   dbfHeader,
		fields:   fields,
		backlink: backlink,
		decoder:  decoder,
		path:     path,
	}
	for _, f := range dbf.fields {
		if f.Name == "_NullFlags" {
			dbf.nullField = &f
			break
		}
	}

	if (dbfHeader.Flags & FlagMemo) != 0 {
		ext := strings.ToUpper(filepath.Ext(path))
		memoExt := ".FPT"
		if ext == ".DBC" {
			memoExt = ".DCT"
		}
		memoFile := filepath.Base(path)
		memoFile = memoFile[:strings.LastIndex(memoFile, ".")] + memoExt

		dbf.memoFile = filepath.Join(filepath.Dir(path), memoFile)
	}
	return &dbf, nil
}

// Name returns the path that was provided for this table
func (dbf *Dbf) Name() string {
	return dbf.path
}

// DBC returns the DBF's DBC
func (dbf *Dbf) DBC() string {
	if (dbf.header.Flags & FlagDBC) == 0 {
		return dbf.backlink
	}
	return ""
}

// ReadDBC reads the DBC to which this table belongs.
// This updates the internal Fieldnames if they were longer than 10 chars
func (dbf *Dbf) ReadDBC() error {
	if dbf.DBC() == "" {
		return fmt.Errorf("This table does not belong to a DBC")
	}

	absPath, err := filepath.Abs(dbf.Name())
	if err != nil {
		return err
	}
	dbcPath := filepath.Join(filepath.Dir(absPath), dbf.DBC())

	db, err := ReadDBC(dbcPath, dbf.decoder)
	if err != nil {
		return err
	}
	return dbf.ReadFromDBC(db)
}

// ReadFromDBC reads the DBC.
// This updates the internal Fieldnames if they were longer than 10 chars
func (dbf *Dbf) ReadFromDBC(db *Dbc) error {
	if dbf.DBC() == "" {
		return fmt.Errorf("This table does not belong to a DBC")
	}

	tblName := filepath.Base(dbf.Name())
	tblName = tblName[:strings.LastIndex(tblName, ".")]
	fields, err := db.TableFields(strings.ToUpper(tblName))
	if err != nil {
		return err
	}
	for i, f := range fields {
		dbf.fields[i].Name = f
	}
	return nil
}

func (dbf *Dbf) openMemo() (*os.File, error) {
	return os.Open(dbf.memoFile)
}

// Close closes the underlying DBF
func (dbf *Dbf) Close() error {
	return dbf.dbfFile.Close()
}

// Header returns the DBF header
func (dbf *Dbf) Header() Header {
	return dbf.header
}

// RecordAt reads the record at the specified position
func (dbf *Dbf) RecordAt(recno uint32, handle func(Record)) error {
	if recno >= dbf.header.RecordCount {
		return ErrInvalidRecordNumber
	}

	var err error
	buffer := make([]byte, dbf.header.RecordLength)
	r := &nullRecord{
		simpleRecord: simpleRecord{
			recno:  recno,
			dbf:    dbf,
			buffer: buffer,
		},
	}

	if dbf.memoFile != "" {
		r.memoFile, err = dbf.openMemo()
		if err != nil {
			return fmt.Errorf("Could not open memo file %q. %w", dbf.memoFile, err)
		}
		defer r.memoFile.Close()
		if _, err := r.memoFile.Seek(6, io.SeekStart); err != nil {
			return fmt.Errorf("Invalid memo table size. %w", err)
		}
		intBuf := make([]byte, 2, 2)
		if _, err := r.memoFile.Read(intBuf); err != nil {
			return fmt.Errorf("Could not read memo blocksize. %w", err)
		}
		r.memoBlockSize = int64(binary.BigEndian.Uint16(intBuf))
	}
	handle(r)
	return nil
}

// Scan walks the entire table until the end or walk returns a non nil error
func (dbf *Dbf) Scan(walk func(Record) error) error {
	var err error
	buffer := make([]byte, dbf.header.RecordLength)
	r := &nullRecord{
		simpleRecord: simpleRecord{
			recno:  0,
			dbf:    dbf,
			buffer: buffer,
		},
	}

	if dbf.memoFile != "" {
		r.memoFile, err = dbf.openMemo()
		if err != nil {
			return err
		}
		defer r.memoFile.Close()
		if _, err := r.memoFile.Seek(6, io.SeekStart); err != nil {
			return err
		}
		intBuf := make([]byte, 2, 2)
		r.memoFile.Read(intBuf)
		r.memoBlockSize = int64(binary.BigEndian.Uint16(intBuf))
	}

	for i := uint32(0); i < dbf.header.RecordCount; i++ {
		if err = walk(r); err != nil {
			break
		}
		r.recno = i
		r.read = false
	}
	return err
}

// CalculatedRecordCount returns the calculated RecordCount or -1.
func (dbf *Dbf) CalculatedRecordCount() int {
	fileSize := dbf.dbfFile.Len()

	return (fileSize - int(dbf.header.HeaderSize)) / int(dbf.header.RecordLength)
}
