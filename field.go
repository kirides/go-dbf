package dbf

import (
	"bytes"
	"encoding/binary"
	"io"

	"golang.org/x/text/encoding"
)

const fieldDescriptorTerminator = 0x0D

type FieldFlag byte

const (
	FieldFlagNone          = 0x00
	FieldFlagSystem        = 0x01
	FieldFlagNull          = 0x02
	FieldFlagBinary        = 0x04
	FieldFlagBinaryAndNull = 0x06
	FieldFlagAutoInc       = 0x0C
)

// Field provides methods to access a DBF
type Field struct {
	Name               string
	Type               rune
	Displacement       uint32
	Length             byte
	DecimalCount       byte
	Flags              FieldFlag
	NextAutoIncrement  uint32
	AutoIncrementStep  byte
	Index              int
	VarLengthSizeIndex int
	NullFieldIndex     int
}

func readFields(r io.ReaderAt, decoder *encoding.Decoder) ([]Field, error) {
	var fields []Field
	buf := make([]byte, 32, 32)

	index := 0
	nullFieldIndex := -1
	for {
		if _, err := r.ReadAt(buf, (int64(index)+1)*32); err != nil {
			return nil, err
		}
		if buf[0] == fieldDescriptorTerminator {
			break
		}
		f := Field{}

		f.Name, _ = decoder.String(string(buf[:bytes.IndexByte(buf, 0x00)]))
		f.Type = rune(buf[11])
		f.Displacement = binary.LittleEndian.Uint32(buf[12:])
		f.Length = buf[16]
		f.DecimalCount = buf[17]
		f.Flags = FieldFlag(buf[18])
		f.NextAutoIncrement = binary.LittleEndian.Uint32(buf[19:])
		f.AutoIncrementStep = buf[23]
		f.Index = index

		f.VarLengthSizeIndex = -1
		f.NullFieldIndex = -1

		if f.Type == 'V' || f.Type == 'Q' {
			nullFieldIndex++
			f.VarLengthSizeIndex = nullFieldIndex
		}
		if (f.Flags & FieldFlagNull) != 0 {
			nullFieldIndex++
			f.NullFieldIndex = nullFieldIndex
		}

		index++

		fields = append(fields, f)
	}

	return fields, nil
}
