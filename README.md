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

## Mapped datatypes
- `C` -> string
- `M` -> string
- `D` -> time.Time (in local timezone)
- `T` -> time.Time (in local timezone)
- `I` -> uint32
- `L` -> bool
- `N`
    - No decimals: int64
    - Decimals: float64

### Currently unsupported datatypes
- `V` Varchar
- `G` General (COM)
- `Q` Binary
- `B` Double
