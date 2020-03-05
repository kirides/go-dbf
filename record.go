package dbf

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"strconv"
	"time"
)

// Record provides methods to work with record
type Record interface {
	Recno() uint32
	Deleted() bool
	ToMap() (map[string]interface{}, error)
}

type simpleRecord struct {
	recno   uint32
	deleted bool

	buffer        []byte
	memoBuffer    []byte
	dbf           *Dbf
	memoFile      *os.File
	memoBlockSize int64
	read          bool
}

func (r *simpleRecord) Recno() uint32 {
	return r.recno
}
func (r *simpleRecord) parse() {
	if r.read {
		return
	}
	r.dbf.dbfFile.Read(r.buffer)

	r.read = true
}

// Deleted returns a bool that tells if a record is marked as deleted or not
func (r *simpleRecord) Deleted() bool {
	if !r.read {
		r.parse()
	}

	return r.buffer[0] == 0x2A
}

// ToMap parses the record into a map[string]interface{}
func (r *simpleRecord) ToMap() (map[string]interface{}, error) {
	var err error
	if !r.read {
		r.parse()
	}
	m := make(map[string]interface{})

	for _, f := range r.dbf.fields {
		// Skip internal columns
		if (f.Flags & FieldFlagSystem) != 0 {
			continue
		}

		switch f.Type {
		case 'I':
			m[f.Name] = binary.LittleEndian.Uint32(r.buffer[f.Displacement:])
		case 'C':
			m[f.Name], _ = r.dbf.decoder.String(string(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)]))
		case 'D':
			m[f.Name], _ = time.Parse("20060102", string(r.buffer[f.Displacement:f.Displacement+uint32(f.Length)]))
		case 'T':
			m[f.Name] = julianDateTimeToTime(binary.LittleEndian.Uint64(r.buffer[f.Displacement:]))
		case 'L':
			v := r.buffer[f.Displacement]
			if v > 0 {
				m[f.Name] = true
			} else {
				m[f.Name] = false
			}
		case 'N':
			if f.DecimalCount == 0 {
				m[f.Name], _ = strconv.ParseInt(string(r.buffer[f.Displacement:f.Displacement+uint32(f.Length)]), 10, 64)
			} else {
				m[f.Name], _ = strconv.ParseFloat(string(r.buffer[f.Displacement:f.Displacement+uint32(f.Length)]), 64)
			}
		case 'M':
			offset := binary.LittleEndian.Uint32(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)])
			if offset == 0 {
				m[f.Name] = ""
				continue
			}
			_, err = r.memoFile.Seek(4+int64(offset)*r.memoBlockSize, io.SeekStart)
			if err != nil {
				return nil, err
			}
			intBuf := make([]byte, 4, 4)
			_, err = r.memoFile.Read(intBuf)
			if err != nil {
				return nil, err
			}
			memoSize := int(binary.BigEndian.Uint32(intBuf))
			if memoSize == 0 {
				m[f.Name] = ""
				continue
			}
			if len(r.memoBuffer) < memoSize {
				r.memoBuffer = make([]byte, memoSize, memoSize)
			}
			_, err = r.memoFile.Read(r.memoBuffer[:memoSize])
			if err != nil {
				return nil, err
			}
			m[f.Name], err = r.dbf.decoder.String(string(r.memoBuffer[:memoSize]))
		}
	}

	return m, nil
}

type nullRecord struct {
	simpleRecord

	nullFlags uint64
}

func (r *nullRecord) parse() {
	if r.read {
		return
	}
	r.dbf.dbfFile.Read(r.buffer)

	if r.dbf.nullField != nil {
		if r.dbf.nullField.Length == 1 {
			r.nullFlags = uint64(r.buffer[r.dbf.nullField.Displacement])
		}
	}

	r.read = true
}

// ToMap parses the record into a map[string]interface{}
func (r *nullRecord) ToMap() (map[string]interface{}, error) {
	var err error
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

		switch f.Type {
		case 'I':
			m[f.Name] = binary.LittleEndian.Uint32(r.buffer[f.Displacement:])
		case 'C':
			m[f.Name], _ = r.dbf.decoder.String(string(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)]))
		case 'D':
			m[f.Name], _ = time.Parse("20060102", string(r.buffer[f.Displacement:f.Displacement+uint32(f.Length)]))
		case 'T':
			m[f.Name] = julianDateTimeToTime(binary.LittleEndian.Uint64(r.buffer[f.Displacement:]))
		case 'N':
			if f.DecimalCount == 0 {
				m[f.Name], _ = strconv.ParseInt(string(r.buffer[f.Displacement:f.Displacement+uint32(f.Length)]), 10, 64)
			} else {
				m[f.Name], _ = strconv.ParseFloat(string(r.buffer[f.Displacement:f.Displacement+uint32(f.Length)]), 64)
			}
		case 'L':
			v := r.buffer[f.Displacement]
			if v > 0 {
				m[f.Name] = true
			} else {
				m[f.Name] = false
			}
		case 'M':
			offset := binary.LittleEndian.Uint32(r.buffer[f.Displacement : f.Displacement+uint32(f.Length)])
			if offset == 0 {
				m[f.Name] = ""
				continue
			}
			_, err = r.memoFile.Seek(4+int64(offset)*r.memoBlockSize, io.SeekStart)
			if err != nil {
				return nil, err
			}
			intBuf := make([]byte, 4, 4)
			_, err = r.memoFile.Read(intBuf)
			if err != nil {
				return nil, err
			}
			memoSize := int(binary.BigEndian.Uint32(intBuf))
			if memoSize == 0 {
				m[f.Name] = ""
				continue
			}
			if len(r.memoBuffer) < memoSize {
				r.memoBuffer = make([]byte, memoSize, memoSize)
			}
			_, err = r.memoFile.Read(r.memoBuffer[:memoSize])
			if err != nil {
				return nil, err
			}
			m[f.Name], err = r.dbf.decoder.String(string(r.memoBuffer[:memoSize]))
		}
	}

	return m, nil
}

var minimumDateTime = time.Date(0001, time.Month(1), 1, 0, 0, 0, 0, time.Local)

// MinimumDateTime returns 0001-01-01T00:00:00Z
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
