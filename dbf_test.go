package dbf

import (
	"testing"

	"golang.org/x/text/encoding/charmap"
)

func Test_LargeDbf(t *testing.T) {
	tbl, err := Open(`test/contacts.dbf`, charmap.Windows1252.NewDecoder())
	if err != nil {
		t.FailNow()
	}
	defer tbl.Close()

	// Oder für bessere Performance, wenn man weiß dass die Tabellen die gleiche DBC haben
	// db := ReadDBC("test/contacts.dbc", charmap.Windows1252.NewDecoder())
	// ...
	// tbl.ReadFromDBC(db)

	if err := tbl.ReadDBC(); err != nil {
		t.Logf("Failed to read DBC: %v\n", err)
		t.FailNow()
	}

	err = tbl.Scan(func(r *Record) error {
		if !r.Deleted() {
			r.ToMap()
			// m, _ := r.ToMap()
			// t.Logf("Record: %v\n", m)
		}
		return nil
	}, 0)
	if err != nil {
		t.FailNow()
	}
}

func Test_RecordAt(t *testing.T) {
	tbl, err := Open(`test/contacts.dbf`, charmap.Windows1252.NewDecoder())
	if err != nil {
		t.FailNow()
	}
	defer tbl.Close()

	if err := tbl.ReadDBC(); err != nil {
		t.Logf("Failed to read DBC: %v\n", err)
		t.FailNow()
	}

	err = tbl.RecordAt(2, func(r *Record) {
		if !r.Deleted() {
			r.ToMap()
			// m, _ := r.ToMap()
			// t.Logf("Record: %v\n", m)
		}
	}, 0)
	if err != nil {
		t.FailNow()
	}
}
