package main

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	xitdb "github.com/xit-vcs/xitdb-go"
)

func formatKey(cursor *xitdb.ReadCursor) (string, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case xitdb.TagNone:
		return "(none)", nil
	case xitdb.TagBytes, xitdb.TagShortBytes:
		b, err := cursor.ReadBytes(0)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%q", string(b)), nil
	case xitdb.TagUint:
		v, err := cursor.ReadUint()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", v), nil
	case xitdb.TagInt:
		v, err := cursor.ReadInt()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", v), nil
	case xitdb.TagFloat:
		v, err := cursor.ReadFloat()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%g", v), nil
	default:
		return fmt.Sprintf("<key tag: %d>", cursor.SlotPtr.Slot.Tag), nil
	}
}

func getKeyValue(cursor *xitdb.ReadCursor) (any, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case xitdb.TagBytes, xitdb.TagShortBytes:
		b, err := cursor.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case xitdb.TagUint:
		v, err := cursor.ReadUint()
		if err != nil {
			return nil, err
		}
		return v, nil
	case xitdb.TagInt:
		v, err := cursor.ReadInt()
		if err != nil {
			return nil, err
		}
		return v, nil
	case xitdb.TagFloat:
		v, err := cursor.ReadFloat()
		if err != nil {
			return nil, err
		}
		return v, nil
	default:
		return fmt.Sprintf("<key tag: %d>", cursor.SlotPtr.Slot.Tag), nil
	}
}

func toJsonValue(cursor *xitdb.ReadCursor, isRoot bool) (any, error) {
	switch cursor.SlotPtr.Slot.Tag {
	case xitdb.TagNone:
		return nil, nil

	case xitdb.TagArrayList:
		list, err := xitdb.NewReadArrayList(cursor)
		if err != nil {
			return nil, err
		}
		count, err := list.Count()
		if err != nil {
			return nil, err
		}
		if isRoot {
			itemCursor, err := list.GetCursor(count - 1)
			if err != nil {
				return nil, err
			}
			return toJsonValue(itemCursor, false)
		}
		result := make([]any, 0, count)
		for i := int64(0); i < count; i++ {
			itemCursor, err := list.GetCursor(i)
			if err != nil {
				return nil, err
			}
			v, err := toJsonValue(itemCursor, false)
			if err != nil {
				return nil, err
			}
			result = append(result, v)
		}
		return result, nil

	case xitdb.TagHashMap, xitdb.TagCountedHashMap:
		result := make(map[string]any)
		for c, err := range cursor.All() {
			if err != nil {
				return nil, err
			}
			kvPair, err := c.ReadKeyValuePair()
			if err != nil {
				return nil, err
			}
			key, err := getKeyValue(kvPair.KeyCursor)
			if err != nil {
				return nil, err
			}
			val, err := toJsonValue(kvPair.ValueCursor, false)
			if err != nil {
				return nil, err
			}
			result[fmt.Sprintf("%v", key)] = val
		}
		return result, nil

	case xitdb.TagHashSet, xitdb.TagCountedHashSet:
		var result []any
		for c, err := range cursor.All() {
			if err != nil {
				return nil, err
			}
			kvPair, err := c.ReadKeyValuePair()
			if err != nil {
				return nil, err
			}
			key, err := getKeyValue(kvPair.KeyCursor)
			if err != nil {
				return nil, err
			}
			result = append(result, key)
		}
		return result, nil

	case xitdb.TagLinkedArrayList:
		var result []any
		for c, err := range cursor.All() {
			if err != nil {
				return nil, err
			}
			v, err := toJsonValue(c, false)
			if err != nil {
				return nil, err
			}
			result = append(result, v)
		}
		return result, nil

	case xitdb.TagBytes, xitdb.TagShortBytes:
		bytesObj, err := cursor.ReadBytesObject(0)
		if err != nil {
			return nil, err
		}
		text := string(bytesObj.Value)
		if isPrintable(text) {
			return text, nil
		}
		return map[string]any{"_binary": base64.StdEncoding.EncodeToString(bytesObj.Value)}, nil

	case xitdb.TagUint:
		return cursor.ReadUint()

	case xitdb.TagInt:
		return cursor.ReadInt()

	case xitdb.TagFloat:
		return cursor.ReadFloat()

	default:
		return map[string]any{"_unknown": int(cursor.SlotPtr.Slot.Tag)}, nil
	}
}

func printValue(cursor *xitdb.ReadCursor, indent string) error {
	switch cursor.SlotPtr.Slot.Tag {
	case xitdb.TagNone:
		fmt.Printf("%s(none)\n", indent)

	case xitdb.TagArrayList:
		list, err := xitdb.NewReadArrayList(cursor)
		if err != nil {
			return err
		}
		count, err := list.Count()
		if err != nil {
			return err
		}
		fmt.Printf("%sArrayList[%d]:\n", indent, count)
		if indent == "" {
			itemCursor, err := list.GetCursor(count - 1)
			if err != nil {
				return err
			}
			return printValue(itemCursor, indent+"    ")
		}
		for i := int64(0); i < count; i++ {
			itemCursor, err := list.GetCursor(i)
			if err != nil {
				return err
			}
			fmt.Printf("%s  [%d]:\n", indent, i)
			if err := printValue(itemCursor, indent+"    "); err != nil {
				return err
			}
		}

	case xitdb.TagHashMap, xitdb.TagCountedHashMap:
		type entry struct {
			key         string
			valueCursor *xitdb.ReadCursor
		}
		var entries []entry
		for c, err := range cursor.All() {
			if err != nil {
				return err
			}
			kvPair, err := c.ReadKeyValuePair()
			if err != nil {
				return err
			}
			key, err := formatKey(kvPair.KeyCursor)
			if err != nil {
				return err
			}
			entries = append(entries, entry{key, kvPair.ValueCursor})
		}
		prefix := "HashMap"
		if cursor.SlotPtr.Slot.Tag == xitdb.TagCountedHashMap {
			prefix = "CountedHashMap"
		}
		fmt.Printf("%s%s{%d}:\n", indent, prefix, len(entries))
		for _, e := range entries {
			fmt.Printf("%s  %s:\n", indent, e.key)
			if err := printValue(e.valueCursor, indent+"    "); err != nil {
				return err
			}
		}

	case xitdb.TagHashSet, xitdb.TagCountedHashSet:
		var keys []string
		for c, err := range cursor.All() {
			if err != nil {
				return err
			}
			kvPair, err := c.ReadKeyValuePair()
			if err != nil {
				return err
			}
			key, err := formatKey(kvPair.KeyCursor)
			if err != nil {
				return err
			}
			keys = append(keys, key)
		}
		prefix := "HashSet"
		if cursor.SlotPtr.Slot.Tag == xitdb.TagCountedHashSet {
			prefix = "CountedHashSet"
		}
		fmt.Printf("%s%s{%d}: [%s]\n", indent, prefix, len(keys), strings.Join(keys, ", "))

	case xitdb.TagLinkedArrayList:
		count, err := cursor.Count()
		if err != nil {
			return err
		}
		fmt.Printf("%sLinkedArrayList[%d]:\n", indent, count)
		i := 0
		for c, err := range cursor.All() {
			if err != nil {
				return err
			}
			fmt.Printf("%s  [%d]:\n", indent, i)
			if err := printValue(c, indent+"    "); err != nil {
				return err
			}
			i++
		}

	case xitdb.TagBytes, xitdb.TagShortBytes:
		bytesObj, err := cursor.ReadBytesObject(0)
		if err != nil {
			return err
		}
		text := string(bytesObj.Value)
		printable := isPrintable(text)

		if printable && len(text) <= 100 {
			if len(bytesObj.FormatTag) > 0 {
				fmt.Printf("%s%q (format: %s)\n", indent, text, string(bytesObj.FormatTag))
			} else {
				fmt.Printf("%s%q\n", indent, text)
			}
		} else {
			preview := bytesObj.Value
			if len(preview) > 16 {
				preview = preview[:16]
			}
			hex := formatHex(preview)
			if len(bytesObj.FormatTag) > 0 {
				fmt.Printf("%s<%d bytes: %s...> (format: %s)\n", indent, len(bytesObj.Value), hex, string(bytesObj.FormatTag))
			} else {
				fmt.Printf("%s<%d bytes: %s...>\n", indent, len(bytesObj.Value), hex)
			}
		}

	case xitdb.TagUint:
		v, err := cursor.ReadUint()
		if err != nil {
			return err
		}
		fmt.Printf("%s%d (uint)\n", indent, v)

	case xitdb.TagInt:
		v, err := cursor.ReadInt()
		if err != nil {
			return err
		}
		fmt.Printf("%s%d (int)\n", indent, v)

	case xitdb.TagFloat:
		v, err := cursor.ReadFloat()
		if err != nil {
			return err
		}
		fmt.Printf("%s%g (float)\n", indent, v)

	default:
		fmt.Printf("%s<unknown tag: %d>\n", indent, cursor.SlotPtr.Slot.Tag)
	}
	return nil
}

func isPrintable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r == '\n' || r == '\r' || r == '\t' {
			continue
		}
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func formatHex(b []byte) string {
	parts := make([]string, len(b))
	for i, v := range b {
		parts[i] = fmt.Sprintf("%02x", v)
	}
	return strings.Join(parts, " ")
}

func hasherFromHeader(header xitdb.Header) (xitdb.Hasher, error) {
	id := xitdb.IDToString(header.HashID)
	switch id {
	case "sha1":
		return xitdb.Hasher{
			Hash: sha1.New(),
			ID:   header.HashID,
		}, nil
	case "sha2":
		switch header.HashSize {
		case 32:
			return xitdb.Hasher{
				Hash: sha256.New(),
				ID:   header.HashID,
			}, nil
		case 64:
			return xitdb.Hasher{
				Hash: sha512.New(),
				ID:   header.HashID,
			}, nil
		default:
			return xitdb.Hasher{}, fmt.Errorf("unsupported sha2 hash size: %d", header.HashSize)
		}
	default:
		// Fall back to SHA-1 for unknown/zero hash IDs
		return xitdb.Hasher{
			Hash: sha1.New(),
			ID:   header.HashID,
		}, nil
	}
}

func main() {
	args := os.Args[1:]
	jsonFlag := false
	var fileArgs []string
	for _, arg := range args {
		if arg == "--json" {
			jsonFlag = true
		} else {
			fileArgs = append(fileArgs, arg)
		}
	}

	if len(fileArgs) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: dump-database [--json] <database-file>\n")
		os.Exit(1)
	}

	filePath := fileArgs[0]

	f, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	core := xitdb.NewCoreBufferedFile(f)

	header, err := xitdb.ReadHeader(core)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading header: %v\n", err)
		os.Exit(1)
	}

	hasher, err := hasherFromHeader(header)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	db, err := xitdb.NewDatabase(core, hasher)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading database: %v\n", err)
		os.Exit(1)
	}

	rc := db.RootCursor().ReadCursor

	if jsonFlag {
		v, err := toJsonValue(rc, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(v); err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Database: %s\n", filePath)
		fmt.Println("---")
		if err := printValue(rc, ""); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}
}
