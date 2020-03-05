package dbf

import "time"

// Type specifies the table type
type Type byte

const (
	// TypeNone ...
	TypeNone Type = 0x00
	// TypeFoxBase ...
	TypeFoxBase Type = 0x02
	// TypeFoxBasePlusDBaseIII ...
	TypeFoxBasePlusDBaseIII Type = 0x03
	// TypeVisualFoxPro ...
	TypeVisualFoxPro Type = 0x30
	// TypeVisualFoxProAutoInc ...
	TypeVisualFoxProAutoInc Type = 0x31
	// TypeVisualFoxProVar ...
	TypeVisualFoxProVar Type = 0x32
	// TypeDBaseIVTable ...
	TypeDBaseIVTable Type = 0x43
	// TypeDBaseIVSystem ...
	TypeDBaseIVSystem Type = 0x63
	// TypeFoxBasePlusDBaseIIIMemo ...
	TypeFoxBasePlusDBaseIIIMemo Type = 0x83
	// TypeDBaseIVMemo ...
	TypeDBaseIVMemo Type = 0x8B
	// TypeDBaseIVTableMemo ...
	TypeDBaseIVTableMemo Type = 0xCB
	// TypeFoxPro2Memo ...
	TypeFoxPro2Memo Type = 0xF5
	// TypeFoxBase2 ...
	TypeFoxBase2 Type = 0xFB
)

// Flag defines flags
type Flag byte

const (
	// FlagNone no flags specified
	FlagNone Flag = 0x00
	// FlagCDX File has a supporting structural index
	FlagCDX Flag = 0x01
	// FlagMemo File has a supporting Memo file
	FlagMemo Flag = 0x02
	// FlagDBC File is part of a DBC
	FlagDBC Flag = 0x04
)

// Header defines the DBF header
type Header struct {
	Type         Type
	ModYear      byte
	ModMonth     byte
	ModDay       byte
	RecordCount  uint32
	HeaderSize   uint16
	RecordLength uint16
	Reserved     [16]byte
	Flags        Flag
	CodePage     byte
}

// LastModified returns the last modification date
func (h Header) LastModified() time.Time {
	return time.Date(int(h.ModYear), time.Month(h.ModMonth), int(h.ModDay), 0, 0, 0, 0, time.Local)
}
