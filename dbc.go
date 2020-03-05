package dbf

import (
	"fmt"
	"strings"

	"golang.org/x/text/encoding"
)

func ReadDBC(path string, decoder *encoding.Decoder) (*Dbc, error) {
	dbcDbf, err := Open(path, decoder)
	if err != nil {
		return nil, err
	}
	defer dbcDbf.Close()

	tables := make(map[string][]string)
	tablesByID := make(map[uint32]string)

	dbcDbf.Scan(func(r Record) error {
		if !r.Deleted() {
			m, err := r.ToMap()
			if err != nil {
				return err
			}
			if m["OBJECTTYPE"].(string) == "Table     " {
				tablesByID[m["OBJECTID"].(uint32)] = strings.ToUpper(strings.TrimSpace(m["OBJECTNAME"].(string)))
			} else if m["OBJECTTYPE"].(string) == "Field     " {
				parentID := m["PARENTID"].(uint32)
				parentName := tablesByID[parentID]
				tables[parentName] = append(tables[parentName], strings.ToUpper(strings.TrimSpace(m["OBJECTNAME"].(string))))
			}
		}
		return nil
	}, 0)

	return &Dbc{
		tables: tables,
	}, nil
}

// Dbc is a database for Visual FoxPro tables
type Dbc struct {
	tables map[string][]string
}

// TableFields returns the fields of a table
func (db *Dbc) TableFields(name string) ([]string, error) {
	fields, ok := db.tables[name]
	if !ok {
		return nil, fmt.Errorf("Table %q not found.", name)
	}
	return fields, nil
}
