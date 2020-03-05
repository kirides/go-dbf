package dbf

import (
	"testing"

	"golang.org/x/text/encoding/charmap"
)

func TestReadDBC(t *testing.T) {
	_, err := ReadDBC("test/contacts.dbc", charmap.Windows1252.NewDecoder())
	if err != nil {
		t.Logf("Could not Read DBC. %v", err)
		t.FailNow()
	}

}

func TestDbcTableFields(t *testing.T) {
	db, _ := ReadDBC("test/contacts.dbc", charmap.Windows1252.NewDecoder())

	_, err := db.TableFields("CONTACTS")
	if err != nil {
		t.FailNow()
	}
}
