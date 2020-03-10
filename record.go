package dbf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"go4.org/strutil"
)

var bufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 4096) },
}

func getBuffer(minSize int) []byte {
	b := bufferPool.Get().([]byte)
	if len(b) < minSize {
		b = make([]byte, minSize)
	}

	return b
}
func putBuffer(b []byte) {
	bufferPool.Put(b)
}

var sBufferPool = sync.Pool{
	New: func() interface{} { return make([]interface{}, 20) },
}

func getSliceBuffer(minSize int) []interface{} {
	b := sBufferPool.Get().([]interface{})
	if len(b) < minSize {
		b = make([]interface{}, minSize)
	}

	return b
}
func putSliceBuffer(b []interface{}) {
	sBufferPool.Put(b)
}

// Record provides methods to work with record
type Record struct {
	recno   uint32
	deleted bool

	buffer       []byte
	dbf          *Dbf
	read         bool
	parseOptions ParseOption

	nullFlags uint64
	intBuf    [4]byte
}

func newRecord(dbf *Dbf, recno uint32, parseOptions ParseOption) *Record {
	return &Record{
		recno:        recno,
		dbf:          dbf,
		buffer:       getBuffer(int(dbf.header.RecordLength)),
		parseOptions: parseOptions,
	}
}

// Deleted returns a bool that tells if a record is marked as deleted or not
func (r *Record) Deleted() bool {
	if !r.read {
		r.parse()
	}

	return r.buffer[0] == 0x2A
}

// Recno returns the record number for the current record
func (r *Record) Recno() uint32 {
	return r.recno
}

func (r *Record) parse() {
	if r.read {
		return
	}
	r.dbf.dbfFile.Read(r.buffer[:r.dbf.header.RecordLength])

	if r.dbf.nullField != nil {
		if r.dbf.nullField.Length == 1 {
			r.nullFlags = uint64(r.buffer[r.dbf.nullField.Displacement])
		}
	}

	r.read = true
}

// ToMap parses the record into a map[string]interface{}
func (r *Record) ToMap() (map[string]interface{}, error) {
	if !r.read {
		r.parse()
	}
	m := make(map[string]interface{})

	for _, f := range r.dbf.fields {
		// Skip internal columns
		if (f.Flags & FieldFlagSystem) != 0 {
			continue
		}
		if f.NullFieldIndex != -1 {
			if (r.nullFlags & (1 << f.NullFieldIndex)) != 0 {
				m[f.Name] = nil
				continue
			}
		}

		v, ok, err := r.parseField(&f)
		if err != nil {
			return nil, err
		}
		if ok {
			m[f.Name] = v
		}
	}

	return m, nil
}

// FieldAt returns a value for that specific field
func (r *Record) FieldAt(fieldIndex int) (interface{}, error) {
	if !r.read {
		r.parse()
	}
	if fieldIndex < 0 || fieldIndex >= len(r.dbf.fields) {
		return nil, fmt.Errorf("FieldAt: Index out of range")
	}

	v, ok, err := r.parseField(&r.dbf.fields[fieldIndex])
	if err != nil {
		return nil, err
	}
	if ok {
		return v, nil
	}
	return nil, fmt.Errorf("FieldAt: Error parsing value")
}

// Field returns a value for that specific field
func (r *Record) Field(fieldName string) (interface{}, error) {
	if !r.read {
		r.parse()
	}
	for i := 0; i < len(r.dbf.fields); i++ {
		if r.dbf.fields[i].Name == fieldName {
			v, ok, err := r.parseField(&r.dbf.fields[i])
			if err != nil {
				return nil, err
			}
			if ok {
				return v, nil
			}
			break
		}
	}
	return nil, fmt.Errorf("Field not found %s", fieldName)
}

// ToSlice parses the record into a []interface{}
func (r *Record) ToSlice() ([]interface{}, error) {
	if !r.read {
		r.parse()
	}
	m := make([]interface{}, len(r.dbf.fields))

	for i, f := range r.dbf.fields {
		// Skip internal columns
		if (f.Flags & FieldFlagSystem) != 0 {
			continue
		}
		if f.NullFieldIndex != -1 {
			if (r.nullFlags & (1 << f.NullFieldIndex)) != 0 {
				m[i] = nil
				continue
			}
		}
		v, ok, err := r.parseField(&f)
		if err != nil {
			return nil, err
		}
		if ok {
			m[i] = v
		}
	}

	return m, nil
}

// WithSlice parses the record into a []interface{}
// The slice is only valid within the current Scan
func (r *Record) WithSlice(sf func([]interface{})) error {
	if !r.read {
		r.parse()
	}
	m := getSliceBuffer(len(r.dbf.fields))
	defer func() {
		putSliceBuffer(m)
	}()
	for i, f := range r.dbf.fields {
		// Skip internal columns
		if (f.Flags & FieldFlagSystem) != 0 {
			continue
		}
		if f.NullFieldIndex != -1 {
			if (r.nullFlags & (1 << f.NullFieldIndex)) != 0 {
				m[i] = nil
				continue
			}
		}
		v, ok, err := r.parseField(&f)
		if err != nil {
			return err
		}
		if ok {
			m[i] = v
		} else {
			m[i] = nil
		}
	}
	sf(m[:len(r.dbf.fields)])
	return nil
}

func (r *Record) parseField(f *Field) (interface{}, bool, error) {

	trimRight := (r.parseOptions & ParseTrimRight) != 0
	switch f.Type {
	case 'I':
		return binary.LittleEndian.Uint32(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)]), true, nil
	case 'C':
		tBuf := getBuffer(int(f.Length) * 4)
		defer func() {
			putBuffer(tBuf)
		}()
		nDst, _, _ := r.dbf.decoder.Transform(tBuf, r.buffer[f.Displacement:f.Displacement+uint32(f.Length)], true)

		v := tBuf[:nDst]
		if trimRight {
			v = bytes.TrimRight(v, " ")
		}
		return string(v), true, nil
	case 'D':
		v, _ := parseDateBytesYYYYMMDD(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)])
		// v, _ := time.Parse("20060102", string(r.buffer[f.Displacement:f.Displacement+uint32(f.Length)]))
		return v, true, nil
	case 'T':
		return julianDateTimeToTime(binary.LittleEndian.Uint64(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)])), true, nil
	case 'N':
		b := r.buffer[f.Displacement : f.Displacement+uint32(f.Length)]
		idxSpace := bytes.LastIndexByte(b, 32)
		if idxSpace >= 0 {
			if idxSpace == int(f.Length-1) {
				b = b[0:0]
			} else {
				b = b[idxSpace+1:]
			}
		}
		if f.DecimalCount == 0 {
			if len(b) == 0 {
				return int64(0), true, nil
			}
			v, _ := strutil.ParseUintBytes(b, 10, 64)
			return int64(v), true, nil
		}
		if len(b) == 0 {
			return float64(0), true, nil
		}
		v, _ := strconv.ParseFloat(string(b), 64)
		return v, true, nil
	case 'L':
		v := r.buffer[f.Displacement]
		if v != 32 && v > 0 {
			return true, true, nil
		}
		return false, true, nil
	case 'M':
		offset := binary.LittleEndian.Uint32(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)])
		if offset == 0 {
			return "", true, nil
		}
		_, err := r.dbf.memoFile.Seek(4+int64(offset)*r.dbf.memoBlockSize, io.SeekStart)
		if err != nil {
			return nil, false, err
		}
		_, err = r.dbf.memoFile.Read(r.intBuf[:])
		if err != nil {
			return nil, false, err
		}
		memoSize := int(binary.BigEndian.Uint32(r.intBuf[:]))
		if memoSize == 0 {
			return "", true, nil
		}
		memoBuffer := getBuffer(memoSize)
		tMemoBuffer := getBuffer(memoSize * 13 / 10)
		defer func() {
			putBuffer(memoBuffer)
			putBuffer(tMemoBuffer)
		}()
		_, err = r.dbf.memoFile.Read(memoBuffer[:memoSize])
		if err != nil {
			return nil, false, err
		}
		nDst, _, _ := r.dbf.decoder.Transform(tMemoBuffer, memoBuffer[:memoSize], true)
		v := tMemoBuffer[:nDst]
		return string(v), true, nil
	}
	return nil, false, nil
}

var minimumDateTime = time.Date(0001, time.Month(1), 1, 0, 0, 0, 0, time.Local)

// MinimumDateTime returns 0001-01-01T00:00:00 @ time.Local
func MinimumDateTime() time.Time {
	return minimumDateTime
}

func julianDateTimeToTime(dateTime uint64) time.Time {
	if dateTime == 0 {
		return MinimumDateTime()
	}

	dt := float64(int32(dateTime))
	t := int(dateTime >> 32)

	// DATE PORTION

	s1 := dt + 68569
	n := math.Floor(4 * s1 / 146097)
	s2 := s1 - math.Floor(((146097*n)+3)/4)
	i := math.Floor(4000 * (s2 + 1) / 1461001)
	s3 := s2 - math.Floor(1461*i/4) + 31
	q := math.Floor(80 * s3 / 2447)
	d := s3 - math.Floor(2447*q/80)
	s4 := math.Floor(q / 11)
	m := q + 2 - (12 * s4)
	j := (100 * (n - 49)) + i + s4

	// TIME PORTION

	hour := t / 3600000
	t -= hour * 3600000
	min := t / 60000
	t -= min * 60000
	sec := t / 1000

	return time.Date(int(j), time.Month(int(m)), int(d), hour, min, sec, 0, time.Local)
}

var emptyDateBytes = []byte{32, 32, 32, 32, 32, 32, 32, 32}

// parseDateBytesYYYYMMDD parses a simple yyyyMMdd format into local-time time.Time
// https://stackoverflow.com/a/27217269
func parseDateBytesYYYYMMDD(date []byte) (time.Time, error) {
	if bytes.Equal(date, emptyDateBytes) {
		return MinimumDateTime(), nil
	}
	year := (((int(date[0])-'0')*10+int(date[1])-'0')*10+int(date[2])-'0')*10 + int(date[3]) - '0'
	month := time.Month((int(date[4])-'0')*10 + int(date[5]) - '0')
	day := (int(date[6])-'0')*10 + int(date[7]) - '0'
	return time.Date(year, month, day, 0, 0, 0, 0, time.Local), nil
}
