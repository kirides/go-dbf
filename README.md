## Opening a DBF to read from it

```go
package main

import (
    "github.com/Kirides/go-dbf"
    "golang.org/x/text/encoding/charmap"
)

func main() {
    db, err := dbf.Open(`C:\Path\To\Some.dbf`, charmap.Windows1252.NewDecoder())
    if err != nil {
        panic(err)
    }
    defer db.Close()
}
```

## Table scan
```go
err = db.Scan(func(r dbf.Record) error {
    if !r.Deleted() {
        _, err = r.ToMap() // returns a map[string]interface{}
        if err != nil {
            panic(err)
        }
    }
    return nil
})

if err != nil {
    panic(err)
}
```
## Reading a specific record
```go
// recno is zero based
err := db.RecordAt(/*recno:*/ 5, func(r dbf.Record) {
    if !r.Deleted() {
        _, err = r.ToMap() // returns a map[string]interface{}
        if err != nil {
            panic(err)
        }
    }
})

if err != nil {
    panic(err)
}
```

## Extened fieldnames from DBC
### Quick and easy
```go
// Read the accompanying DBC file
err := db.ReadDBC()
```

### Pre-read a DBC for reuse with multiple tables
```go
// Read a DBC that lies relative to a .dbf
dbc, err := dbf.ReadDBC(filepath.Join(`location/of/dbf/`), db.DBC()), charmap.Windows1252.NewDecoder())

err := db.ReadFromDBC(dbc)
```
Example of reusing a dbc

```go
knownDbcs := make(map[string]*dbf.Dbc)
dec := charmap.Windows1252.NewDecoder()

db, _ := dbf.Open(`...`, dec)

if db.DBC() != "" {
    if d, ok := knownDbcs[db.DBC()]; ok {
        db.ReadFromDBC(d)
    } else {
        dbc, err := dbf.ReadDBC(
                    filepath.Join(
                        filepath.Dir(dbfPath), db.DBC()),
                    dec)
        // ...
        db.ReadFromDBC(dbc)
        knownDbcs[db.DBC()] = dbc
    }
}
```

## Mapped datatypes
- `C` -> string
- `V` -> string (basic support, might fail on tables with large amount of nullables and/or varchars)
- `M` -> string
- `D` -> time.Time (in local timezone)
- `T` -> time.Time (in local timezone)
- `I` -> uint32
- `L` -> bool
- `N`
    - No decimals: int64
    - Decimals: float64

### Currently unsupported datatypes
- `G` General (COM)
- `Q` Binary
- `B` Double
