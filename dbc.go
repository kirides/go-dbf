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

	objTypeField, err := dbcDbf.FieldByName("OBJECTTYPE")
	if err != nil {
		return nil, err
	}
	objIDField, err := dbcDbf.FieldByName("OBJECTID")
	if err != nil {
		return nil, err
	}
	parentIDField, err := dbcDbf.FieldByName("PARENTID")
	if err != nil {
		return nil, err
	}
	objNameField, err := dbcDbf.FieldByName("OBJECTNAME")
	if err != nil {
		return nil, err
	}

	dbcDbf.Scan(func(r *Record) error {
		if !r.Deleted() {
			oType, err := r.FieldAt(objTypeField.Index)

			if err != nil {
				return err
			}

			if oType.(string) == "Table     " {
				oID, err := r.FieldAt(objIDField.Index)
				if err != nil {
					return err
				}
				oName, err := r.FieldAt(objNameField.Index)
				if err != nil {
					return err
				}
				tablesByID[oID.(uint32)] = strings.ToUpper(strings.TrimSpace(oName.(string)))
			} else if oType.(string) == "Field     " {
				parentID, err := r.FieldAt(parentIDField.Index)

				if err != nil {
					return err
				}
				parentName := tablesByID[parentID.(uint32)]

				oName, err := r.FieldAt(objNameField.Index)
				if err != nil {
					return err
				}
				tables[parentName] = append(tables[parentName], strings.ToUpper(strings.TrimSpace(oName.(string))))
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
