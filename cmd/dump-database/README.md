# xitdb Database Dump Tool

A command-line tool for inspecting and dumping the contents of xitdb database files.

## Installation

```bash
go install github.com/xit-vcs/xitdb-go/cmd/dump-database@latest
```

Or build from source:

```bash
go build -o dump-database ./cmd/dump-database
```

## Usage

### Human-readable output (default)

```bash
dump-database <database-file>
```

This displays the database structure in a hierarchical, indented format showing:
- Data types (ArrayList, HashMap, HashSet, LinkedArrayList, etc.)
- Collection sizes
- Key-value pairs
- Primitive values with their types

Example output:
```
Database: mydata.xdb
---
ArrayList[1]:
    HashMap{3}:
      "name":
        "Alice"
      "age":
        30 (int)
      "active":
        "true" (format: bl)
```

### JSON output

```bash
dump-database --json <database-file>
```

Outputs the database content as formatted JSON, suitable for piping to other tools:

```bash
dump-database --json mydata.xdb | jq '.users'
```

Example output:
```json
{
  "name": "Alice",
  "age": 30,
  "active": "true"
}
```

## Testing

Run the tool against the included test database:

```bash
go run ./cmd/dump-database testdata/test.db
go run ./cmd/dump-database --json testdata/test.db
```

## Output Format Details

### Human-readable format

| xitdb Type | Display Format |
|------------|----------------|
| ArrayList | `ArrayList[count]:` |
| LinkedArrayList | `LinkedArrayList[count]:` |
| HashMap | `HashMap{count}:` |
| CountedHashMap | `CountedHashMap{count}:` |
| HashSet | `HashSet{count}: [items]` |
| CountedHashSet | `CountedHashSet{count}: [items]` |
| Bytes | `"text"` or `<N bytes: hex...>` |
| Bytes with format tag | `"text" (format: tag)` |
| UINT | `value (uint)` |
| INT | `value (int)` |
| FLOAT | `value (float)` |
| NONE | `(none)` |

### JSON format

- Arrays and lists become JSON arrays
- Maps become JSON objects
- Sets become JSON arrays
- Strings remain strings
- Numbers remain numbers
- Binary data becomes `{"_binary": "base64-encoded"}`
- Unknown tags become `{"_unknown": tag-number}`
- For root ArrayList (transaction history), only the latest entry is shown
