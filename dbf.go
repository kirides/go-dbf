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

	"golang.org/x/text/encoding"
)

const maxBacklinkLenght = 263

// ErrInvalidRecordNumber is returned whenever a provided record number is invalid
var ErrInvalidRecordNumber = errors.New("Invalid record")

type file interface {
	Close() error
	Read(b []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	ReadAt(b []byte, offset int64) (int, error)
	Name() string
	Stat() (os.FileInfo, error)
}

// Dbf provides methods to access a DBF
type Dbf struct {
	recpointer    int32
	dbfFile       file
	memoFile      file
	memoBlockSize int64
	decoder       *encoding.Decoder

	header    Header
	fields    []Field
	backlink  string
	nullField *Field
}

// Open opens the specifid DBF
func Open(path string, decoder *encoding.Decoder) (*Dbf, error) {
	osF, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	dbfFile := newMmapFile(osF)
	dbfHeader := Header{}

	if err := binary.Read(dbfFile, binary.LittleEndian, &dbfHeader); err != nil {
		dbfFile.Close()
		return nil, fmt.Errorf("Could not open table at %q. %w", path, err)
	}

	fields, err := readFields(dbfFile, decoder)
	if err != nil {
		dbfFile.Close()
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

		memoFile = filepath.Join(filepath.Dir(path), memoFile)

		if memoFile != "" {
			osM, err := os.Open(memoFile)
			if err != nil {
				dbfFile.Close()
				return nil, err
			}
			dbf.memoFile = newMmapFile(osM)
			if _, err := dbf.memoFile.Seek(6, io.SeekStart); err != nil {
				dbfFile.Close()
				dbf.memoFile.Close()
				return nil, err
			}
			intBuf := make([]byte, 2, 2)
			if _, err := readAll(dbf.memoFile, intBuf); err != nil {
				dbfFile.Close()
				dbf.memoFile.Close()
				return nil, err
			}
			dbf.memoBlockSize = int64(binary.BigEndian.Uint16(intBuf))
		}
	}
	return &dbf, nil
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

	absPath, err := filepath.Abs(dbf.dbfFile.Name())
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

	tblName := filepath.Base(dbf.dbfFile.Name())
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

// Close closes the underlying DBF/FPT
func (dbf *Dbf) Close() error {
	if dbf.dbfFile != nil {
		dbf.dbfFile.Close()
		dbf.dbfFile = nil
	}
	if dbf.memoFile != nil {
		dbf.memoFile.Close()
		dbf.memoFile = nil
	}

	return nil
}

// Header returns the DBF header
func (dbf *Dbf) Header() Header {
	return dbf.header
}

// ParseOption options for handling row parsing
type ParseOption byte

const (
	// ParseDefault default options
	ParseDefault ParseOption = 0
	// ParseTrimRight strings.TrimRight(s, " ") is applied to `C`-type fields
	ParseTrimRight ParseOption = 1 << 0
)

// RecordAt reads the record at the specified position
func (dbf *Dbf) RecordAt(recno uint32, handle func(*Record), options ParseOption) error {
	if recno >= dbf.header.RecordCount {
		return ErrInvalidRecordNumber
	}

	var err error
	r := newRecord(dbf, recno, options)

	_, err = dbf.dbfFile.Seek(int64(dbf.header.HeaderSize)+(int64(dbf.header.RecordLength)*int64(recno)), io.SeekStart)
	if err != nil {
		return fmt.Errorf("Invalid record pointer. %w", err)
	}
	handle(r)
	putBuffer(r.buffer)
	return nil
}

// FieldByName returns a field by it name (Case insensitive)
func (dbf *Dbf) FieldByName(name string) (Field, error) {

	for i := 0; i < len(dbf.fields); i++ {
		if strings.EqualFold(dbf.fields[i].Name, name) {
			return dbf.fields[i], nil
		}
	}
	return Field{}, fmt.Errorf("Field not found %q", name)
}

// ScanOffset walks the table starting at `offset` until the end or walk returns a non nil error
func (dbf *Dbf) ScanOffset(offset uint32, walk func(*Record) error, options ParseOption) error {
	var err error
	r := newRecord(dbf, offset, options)

	dbf.dbfFile.Seek(int64(dbf.header.HeaderSize)+(int64(offset)*int64(dbf.header.RecordLength)), io.SeekStart)

	for i := offset; i < dbf.header.RecordCount; i++ {
		r.recno = i
		if err = walk(r); err != nil {
			break
		}
		if !r.read {
			dbf.dbfFile.Seek(int64(dbf.header.RecordLength), io.SeekCurrent)
		}
		r.read = false
	}
	putBuffer(r.buffer)
	return err
}

// Scan walks the entire table until the end or walk returns a non nil error
func (dbf *Dbf) Scan(walk func(*Record) error, options ParseOption) error {
	return dbf.ScanOffset(0, walk, options)
}

// CalculatedRecordCount returns the calculated RecordCount or -1.
func (dbf *Dbf) CalculatedRecordCount() int {
	stat, err := dbf.dbfFile.Stat()
	if err != nil {
		return -1
	}
	fileSize := int(stat.Size())

	return (fileSize - int(dbf.header.HeaderSize)) / int(dbf.header.RecordLength)
}

// readAll will fill the whole buffer or error out
func readAll(r io.Reader, b []byte) (int, error) {
	toRead := len(b)
	read := 0
	for read < toRead {
		n, err := r.Read(b[read:])
		if err != nil {
			return read, err
		}
		read += n
	}
	return read, nil
}
