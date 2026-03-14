package xitdb

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"hash"
	"math/big"
	"os"
	"strconv"
	"testing"
)

func TestLowLevelApi(t *testing.T) {
	// CoreMemory
	{
		core := NewCoreMemory()
		hasher := sha1Hasher()
		testLowLevelApi(t, core, hasher)
	}

	// CoreFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		defer f.Close()

		core := NewCoreFile(f)
		hasher := sha1Hasher()
		testLowLevelApi(t, core, hasher)
	}

	// CoreBufferedFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		defer f.Close()

		core := NewCoreBufferedFileWithSize(f, 1024)
		hasher := sha1Hasher()
		testLowLevelApi(t, core, hasher)
	}
}

func TestLowLevelMemoryOperations(t *testing.T) {
	core := NewCoreMemory()
	hasher := sha1Hasher()
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}

	hashVal := db.digest([]byte("text"))
	textCursor := db.RootCursor()
	textCursor, err = textCursor.WritePath([]PathPart{
		HashMapInitPart{},
		HashMapGetPart{Target: HashMapGetValue{Hash: hashVal}},
	})
	if err != nil {
		t.Fatal(err)
	}

	writer, err := textCursor.Writer()
	if err != nil {
		t.Fatal(err)
	}
	writer.Write([]byte("goodbye, world!"))
	writer.SeekTo(9)
	writer.Write([]byte("cruel world!"))
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := textCursor.Reader()
	if err != nil {
		t.Fatal(err)
	}
	allBytes, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, "goodbye, cruel world!", string(allBytes))
}

func lastSlotData(t *testing.T, cursor *WriteCursor) WriteableData {
	t.Helper()
	slot, err := cursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
	if err != nil {
		t.Fatal(err)
	}
	if slot != nil {
		return *slot
	}
	return nil
}

func testSlice(t *testing.T, core Core, hasher Hasher, originalSize int, sliceOffset int64, sliceSize int64) {
	t.Helper()
	if err := core.SetLength(0); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}
	rootCursor := db.RootCursor()

	lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
	if err != nil {
		t.Fatal(err)
	}
	var slotData WriteableData
	if lastSlot != nil {
		slotData = *lastSlot
	}
	_, err = rootCursor.WritePath([]PathPart{
		ArrayListInit{},
		ArrayListAppend{},
		WriteDataPart{Data: slotData},
		HashMapInitPart{},
		ContextPart{Function: func(cursor *WriteCursor) error {
			values := make([]int64, 0)

			// create list
			for i := 0; i < originalSize; i++ {
				n := int64(i) * 2
				values = append(values, n)
				_, err := cursor.WritePath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
					LinkedArrayListInit{},
					LinkedArrayListAppend{},
					WriteDataPart{Data: UintData{Value: uint64(n)}},
				})
				if err != nil {
					return err
				}
			}

			// slice list
			evenListCursor, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
			})
			if err != nil {
				return err
			}
			evenListSliceCursor, err := cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-slice"))}},
				WriteDataPart{Data: evenListCursor.SlotPtr.Slot},
				LinkedArrayListInit{},
				LinkedArrayListSlicePart{Offset: sliceOffset, Size: sliceSize},
			})
			if err != nil {
				return err
			}

			// check all the values in the new slice
			for i := int64(0); i < sliceSize; i++ {
				val := values[sliceOffset+i]
				rc, err := cursor.ReadPath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-slice"))}},
					LinkedArrayListGet{Index: i},
				})
				if err != nil {
					return err
				}
				n := rc.SlotPtr.Slot.Value
				if val != n {
					t.Fatalf("expected %d, got %d", val, n)
				}
			}

			// check all values in the new slice with an iterator
			{
				i := 0
				for numCursor, err := range evenListSliceCursor.All() {
					if err != nil {
						return err
					}
					uval, err := numCursor.ReadUint()
					if err != nil {
						return err
					}
					if uint64(values[sliceOffset+int64(i)]) != uval {
						t.Fatalf("expected %d, got %d", values[sliceOffset+int64(i)], uval)
					}
					i++
				}
				if int64(i) != sliceSize {
					t.Fatalf("expected %d iterations, got %d", sliceSize, i)
				}
			}

			// there are no extra items
			rc, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-slice"))}},
				LinkedArrayListGet{Index: sliceSize},
			})
			if err != nil {
				return err
			}
			if rc != nil {
				t.Fatal("expected nil cursor")
			}

			// concat the slice with itself
			_, err = cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
				WriteDataPart{Data: evenListSliceCursor.SlotPtr.Slot},
				LinkedArrayListInit{},
				LinkedArrayListConcatPart{List: evenListSliceCursor.SlotPtr.Slot},
			})
			if err != nil {
				return err
			}

			// check all values in the combo list
			comboValues := make([]int64, 0)
			comboValues = append(comboValues, values[sliceOffset:sliceOffset+sliceSize]...)
			comboValues = append(comboValues, values[sliceOffset:sliceOffset+sliceSize]...)
			for i := 0; i < len(comboValues); i++ {
				rc, err := cursor.ReadPath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
					LinkedArrayListGet{Index: int64(i)},
				})
				if err != nil {
					return err
				}
				n := rc.SlotPtr.Slot.Value
				if comboValues[i] != n {
					t.Fatalf("expected %d, got %d", comboValues[i], n)
				}
			}

			// append to the slice
			_, err = cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-slice"))}},
				LinkedArrayListInit{},
				LinkedArrayListAppend{},
				WriteDataPart{Data: UintData{Value: 3}},
			})
			if err != nil {
				return err
			}

			// read the new value from the slice
			rc, err = cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-slice"))}},
				LinkedArrayListGet{Index: -1},
			})
			if err != nil {
				return err
			}
			if rc.SlotPtr.Slot.Value != 3 {
				t.Fatalf("expected 3, got %d", rc.SlotPtr.Slot.Value)
			}

			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testConcat(t *testing.T, core Core, hasher Hasher, listASize int64, listBSize int64) {
	t.Helper()
	if err := core.SetLength(0); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}
	rootCursor := db.RootCursor()

	values := make([]int64, 0)

	lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
	if err != nil {
		t.Fatal(err)
	}
	var slotData WriteableData
	if lastSlot != nil {
		slotData = *lastSlot
	}
	_, err = rootCursor.WritePath([]PathPart{
		ArrayListInit{},
		ArrayListAppend{},
		WriteDataPart{Data: slotData},
		HashMapInitPart{},
		ContextPart{Function: func(cursor *WriteCursor) error {
			// create even list
			_, err := cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
				LinkedArrayListInit{},
			})
			if err != nil {
				return err
			}
			for i := int64(0); i < listASize; i++ {
				n := i * 2
				values = append(values, n)
				_, err := cursor.WritePath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
					LinkedArrayListInit{},
					LinkedArrayListAppend{},
					WriteDataPart{Data: UintData{Value: uint64(n)}},
				})
				if err != nil {
					return err
				}
			}

			// create odd list
			_, err = cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("odd"))}},
				LinkedArrayListInit{},
			})
			if err != nil {
				return err
			}
			for i := int64(0); i < listBSize; i++ {
				n := (i * 2) + 1
				values = append(values, n)
				_, err := cursor.WritePath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("odd"))}},
					LinkedArrayListInit{},
					LinkedArrayListAppend{},
					WriteDataPart{Data: UintData{Value: uint64(n)}},
				})
				if err != nil {
					return err
				}
			}

			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	lastSlot2, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
	if err != nil {
		t.Fatal(err)
	}
	var slotData2 WriteableData
	if lastSlot2 != nil {
		slotData2 = *lastSlot2
	}
	_, err = rootCursor.WritePath([]PathPart{
		ArrayListInit{},
		ArrayListAppend{},
		WriteDataPart{Data: slotData2},
		HashMapInitPart{},
		ContextPart{Function: func(cursor *WriteCursor) error {
			// get the even list
			evenListCursor, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
			})
			if err != nil {
				return err
			}

			// get the odd list
			oddListCursor, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("odd"))}},
			})
			if err != nil {
				return err
			}

			// concat the lists
			comboListCursor, err := cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
				WriteDataPart{Data: evenListCursor.SlotPtr.Slot},
				LinkedArrayListInit{},
				LinkedArrayListConcatPart{List: oddListCursor.SlotPtr.Slot},
			})
			if err != nil {
				return err
			}

			// check all values in the new list
			for i := 0; i < len(values); i++ {
				rc, err := cursor.ReadPath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
					LinkedArrayListGet{Index: int64(i)},
				})
				if err != nil {
					return err
				}
				n := rc.SlotPtr.Slot.Value
				if values[i] != n {
					t.Fatalf("expected %d, got %d", values[i], n)
				}
			}

			// check all values in the new slice with an iterator
			{
				i := 0
				for numCursor, err := range comboListCursor.All() {
					if err != nil {
						return err
					}
					uval, err := numCursor.ReadUint()
					if err != nil {
						return err
					}
					if uint64(values[i]) != uval {
						t.Fatalf("expected %d, got %d", values[i], uval)
					}
					i++
				}
				evenCount, err := evenListCursor.Count()
				if err != nil {
					return err
				}
				oddCount, err := oddListCursor.Count()
				if err != nil {
					return err
				}
				if int64(i) != evenCount+oddCount {
					t.Fatalf("expected %d iterations, got %d", evenCount+oddCount, i)
				}
			}

			// there are no extra items
			rc, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
				LinkedArrayListGet{Index: int64(len(values))},
			})
			if err != nil {
				return err
			}
			if rc != nil {
				t.Fatal("expected nil cursor")
			}

			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testInsertAndRemove(t *testing.T, core Core, hasher Hasher, originalSize int, insertIndex int64) {
	t.Helper()
	if err := core.SetLength(0); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}
	rootCursor := db.RootCursor()

	var insertValue int64 = 12345

	lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
	if err != nil {
		t.Fatal(err)
	}
	var slotData WriteableData
	if lastSlot != nil {
		slotData = *lastSlot
	}
	_, err = rootCursor.WritePath([]PathPart{
		ArrayListInit{},
		ArrayListAppend{},
		WriteDataPart{Data: slotData},
		HashMapInitPart{},
		ContextPart{Function: func(cursor *WriteCursor) error {
			values := make([]int64, 0)

			// create list
			for i := 0; i < originalSize; i++ {
				if int64(i) == insertIndex {
					values = append(values, insertValue)
				}
				n := int64(i) * 2
				values = append(values, n)
				_, err := cursor.WritePath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
					LinkedArrayListInit{},
					LinkedArrayListAppend{},
					WriteDataPart{Data: UintData{Value: uint64(n)}},
				})
				if err != nil {
					return err
				}
			}

			// insert into list
			evenListCursor, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
			})
			if err != nil {
				return err
			}
			evenListInsertCursor, err := cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-insert"))}},
				WriteDataPart{Data: evenListCursor.SlotPtr.Slot},
				LinkedArrayListInit{},
			})
			if err != nil {
				return err
			}
			_, err = evenListInsertCursor.WritePath([]PathPart{
				LinkedArrayListInsertPart{Index: insertIndex},
				WriteDataPart{Data: UintData{Value: uint64(insertValue)}},
			})
			if err != nil {
				return err
			}

			// check all the values in the new list
			for i := 0; i < len(values); i++ {
				val := values[i]
				rc, err := cursor.ReadPath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-insert"))}},
					LinkedArrayListGet{Index: int64(i)},
				})
				if err != nil {
					return err
				}
				n := rc.SlotPtr.Slot.Value
				if val != n {
					t.Fatalf("expected %d, got %d", val, n)
				}
			}

			// check all values in the new list with an iterator
			{
				i := 0
				for numCursor, err := range evenListInsertCursor.All() {
					if err != nil {
						return err
					}
					uval, err := numCursor.ReadUint()
					if err != nil {
						return err
					}
					if uint64(values[i]) != uval {
						t.Fatalf("expected %d, got %d", values[i], uval)
					}
					i++
				}
				if i != len(values) {
					t.Fatalf("expected %d iterations, got %d", len(values), i)
				}
			}

			// there are no extra items
			rc, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-insert"))}},
				LinkedArrayListGet{Index: int64(len(values))},
			})
			if err != nil {
				return err
			}
			if rc != nil {
				t.Fatal("expected nil cursor")
			}

			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	lastSlot2, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
	if err != nil {
		t.Fatal(err)
	}
	var slotData2 WriteableData
	if lastSlot2 != nil {
		slotData2 = *lastSlot2
	}
	_, err = rootCursor.WritePath([]PathPart{
		ArrayListInit{},
		ArrayListAppend{},
		WriteDataPart{Data: slotData2},
		HashMapInitPart{},
		ContextPart{Function: func(cursor *WriteCursor) error {
			values := make([]int64, 0)

			for i := 0; i < originalSize; i++ {
				n := int64(i) * 2
				values = append(values, n)
			}

			// remove inserted value from the list
			evenListInsertCursor, err := cursor.WritePath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-insert"))}},
				LinkedArrayListRemovePart{Index: insertIndex},
			})
			if err != nil {
				return err
			}

			// check all the values in the new list
			for i := 0; i < len(values); i++ {
				val := values[i]
				rc, err := cursor.ReadPath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-insert"))}},
					LinkedArrayListGet{Index: int64(i)},
				})
				if err != nil {
					return err
				}
				n := rc.SlotPtr.Slot.Value
				if val != n {
					t.Fatalf("expected %d, got %d", val, n)
				}
			}

			// check all values in the new list with an iterator
			{
				i := 0
				for numCursor, err := range evenListInsertCursor.All() {
					if err != nil {
						return err
					}
					uval, err := numCursor.ReadUint()
					if err != nil {
						return err
					}
					if uint64(values[i]) != uval {
						t.Fatalf("expected %d, got %d", values[i], uval)
					}
					i++
				}
				if i != len(values) {
					t.Fatalf("expected %d iterations, got %d", len(values), i)
				}
			}

			// there are no extra items
			rc, err := cursor.ReadPath([]PathPart{
				HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even-insert"))}},
				LinkedArrayListGet{Index: int64(len(values))},
			})
			if err != nil {
				return err
			}
			if rc != nil {
				t.Fatal("expected nil cursor")
			}

			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testLowLevelApi(t *testing.T, core Core, hasher Hasher) {
	t.Helper()

	// open and re-open database
	{
		// make empty database
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		_, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}

		// re-open without error
		_, err = NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		if err := core.SeekTo(0); err != nil {
			t.Fatal(err)
		}
		if err := writeByte_(core, 'g'); err != nil {
			t.Fatal(err)
		}

		// re-open with error
		_, err = NewDatabase(core, hasher)
		if !errors.Is(err, ErrInvalidDatabase) {
			t.Fatalf("expected ErrInvalidDatabase, got %v", err)
		}

		// modify the version
		if err := core.SeekTo(0); err != nil {
			t.Fatal(err)
		}
		if err := writeByte_(core, 'x'); err != nil {
			t.Fatal(err)
		}
		if err := core.SeekTo(4); err != nil {
			t.Fatal(err)
		}
		if err := writeShort(core, Version+1); err != nil {
			t.Fatal(err)
		}

		// re-open with error
		_, err = NewDatabase(core, hasher)
		if !errors.Is(err, ErrInvalidVersion) {
			t.Fatalf("expected ErrInvalidVersion, got %v", err)
		}
	}

	// save hash id in header
	{
		hashID, err := StringToID("sha1")
		if err != nil {
			t.Fatal(err)
		}
		hasherWithHashID := Hasher{
			NewHash: func() hash.Hash { return sha1.New() },
			ID:      hashID,
		}

		// make empty database
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		_, err = NewDatabase(core, hasherWithHashID)
		if err != nil {
			t.Fatal(err)
		}

		// read header without initializing database
		if err := core.SeekTo(0); err != nil {
			t.Fatal(err)
		}
		header, err := ReadHeader(core)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int16(20), header.HashSize)
		assertEqual(t, "sha1", IDToString(header.HashID))

		// determine the hashing algorithm
		var determinedHasher Hasher
		switch IDToString(header.HashID) {
		case "sha1":
			determinedHasher = Hasher{
				NewHash: func() hash.Hash { return sha1.New() },
				ID:      header.HashID,
			}
		default:
			t.Fatal("Invalid hash algorithm")
		}
		h := determinedHasher.NewHash()
		assertEqual(t, 20, h.Size())
	}

	// array_list of hash_maps
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		// write foo -> bar with a writer
		fooKey := db.digest([]byte("foo"))
		lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		var slotData WriteableData
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			HashMapInitPart{},
			HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			ContextPart{Function: func(cursor *WriteCursor) error {
				assertEqual(t, TagNone, cursor.GetSlot().Tag)
				writer, err := cursor.Writer()
				if err != nil {
					return err
				}
				writer.Write([]byte("bar"))
				return writer.Finish()
			}},
		})
		if err != nil {
			t.Fatal(err)
		}

		// read foo
		{
			barCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			count, err := barCursor.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(3), count)
			barValue, err := barCursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "bar", string(barValue))
		}

		// read foo from ctx
		{
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
				ContextPart{Function: func(cursor *WriteCursor) error {
					if cursor.GetSlot().Tag == TagNone {
						t.Fatal("expected tag != TagNone")
					}

					value, err := cursor.ReadBytes(ptrInt64(1024))
					if err != nil {
						return err
					}
					assertEqual(t, "bar", string(value))

					barReader, err := cursor.Reader()
					if err != nil {
						return err
					}

					// read into buffer
					barBytes := make([]byte, 10)
					barSize, err := barReader.Read(barBytes)
					if err != nil {
						return err
					}
					assertEqual(t, "bar", string(barBytes[:barSize]))
					if err := barReader.SeekTo(0); err != nil {
						return err
					}
					barSize, err = barReader.Read(barBytes)
					if err != nil {
						return err
					}
					assertEqual(t, 3, barSize)
					assertEqual(t, "bar", string(barBytes[:3]))

					// read one char at a time
					{
						ch := make([]byte, 1)
						if err := barReader.SeekTo(0); err != nil {
							return err
						}

						if err := barReader.ReadFully(ch); err != nil {
							return err
						}
						assertEqual(t, "b", string(ch))

						if err := barReader.ReadFully(ch); err != nil {
							return err
						}
						assertEqual(t, "a", string(ch))

						if err := barReader.ReadFully(ch); err != nil {
							return err
						}
						assertEqual(t, "r", string(ch))

						err := barReader.ReadFully(ch)
						if !errors.Is(err, ErrEndOfStream) {
							t.Fatalf("expected ErrEndOfStream, got %v", err)
						}

						if err := barReader.SeekTo(1); err != nil {
							return err
						}
						b, err := barReader.ReadByte()
						if err != nil {
							return err
						}
						assertEqual(t, byte('a'), b)

						if err := barReader.SeekTo(0); err != nil {
							return err
						}
						b, err = barReader.ReadByte()
						if err != nil {
							return err
						}
						assertEqual(t, byte('b'), b)
					}
					return nil
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// overwrite foo -> baz
		{
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
				ContextPart{Function: func(cursor *WriteCursor) error {
					if cursor.GetSlot().Tag == TagNone {
						t.Fatal("expected tag != TagNone")
					}

					writer, err := cursor.Writer()
					if err != nil {
						return err
					}
					writer.Write([]byte("x"))
					writer.Write([]byte("x"))
					writer.Write([]byte("x"))
					writer.SeekTo(0)
					writer.Write([]byte("b"))
					writer.SeekTo(2)
					writer.Write([]byte("z"))
					writer.SeekTo(1)
					writer.Write([]byte("a"))
					if err := writer.Finish(); err != nil {
						return err
					}

					value, err := cursor.ReadBytes(ptrInt64(1024))
					if err != nil {
						return err
					}
					assertEqual(t, "baz", string(value))
					return nil
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// if error in ctx, db doesn't change
		{
			sizeBefore, err := core.Length()
			if err != nil {
				t.Fatal(err)
			}

			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, writeErr := rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
				ContextPart{Function: func(cursor *WriteCursor) error {
					writer, err := cursor.Writer()
					if err != nil {
						return err
					}
					writer.Write([]byte("this value won't be visible"))
					if err := writer.Finish(); err != nil {
						return err
					}
					return errors.New("intentional error")
				}},
			})
			// we expect an error here
			_ = writeErr

			// read foo
			valueCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			value, err := valueCursor.ReadBytes(nil) // make sure nil max size works
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "baz", string(value))

			// verify that the db is properly truncated back to its original size after error
			sizeAfter, err := core.Length()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, sizeBefore, sizeAfter)
		}

		// write bar -> longstring
		barKey := db.digest([]byte("bar"))
		{
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			barCursor, err := rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := barCursor.WriteValue(NewBytesDataFromString("longstring")); err != nil {
				t.Fatal(err)
			}

			// the slot tag is BYTES because the byte array is > 8 bytes long
			assertEqual(t, TagBytes, barCursor.GetSlot().Tag)

			// writing again returns the same slot
			{
				lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				var slotData WriteableData
				if lastSlot != nil {
					slotData = *lastSlot
				}
				nextBarCursor, err := rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := nextBarCursor.WriteIfEmpty(NewBytesDataFromString("longstring")); err != nil {
					t.Fatal(err)
				}
				assertEqual(t, barCursor.GetSlot().Value, nextBarCursor.GetSlot().Value)
			}

			// writing with write returns a new slot
			{
				lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				var slotData WriteableData
				if lastSlot != nil {
					slotData = *lastSlot
				}
				nextBarCursor, err := rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := nextBarCursor.WriteValue(NewBytesDataFromString("longstring")); err != nil {
					t.Fatal(err)
				}
				if barCursor.GetSlot().Value == nextBarCursor.GetSlot().Value {
					t.Fatal("expected different slot values")
				}
			}
		}

		// read bar
		{
			readBarCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			barValue, err := readBarCursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "longstring", string(barValue))
		}

		// write bar -> shortstr
		{
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			barCursor, err := rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := barCursor.WriteValue(NewBytesDataFromString("shortstr")); err != nil {
				t.Fatal(err)
			}

			// the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long
			assertEqual(t, TagShortBytes, barCursor.GetSlot().Tag)
			count, err := barCursor.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(8), count)

			// make sure that SHORT_BYTES can be read with a reader
			barReader, err := barCursor.Reader()
			if err != nil {
				t.Fatal(err)
			}
			barValue := make([]byte, count)
			if err := barReader.ReadFully(barValue); err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "shortstr", string(barValue))
		}

		// write bytes with a format tag
		{
			// shortstr
			{
				lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				var slotData WriteableData
				if lastSlot != nil {
					slotData = *lastSlot
				}
				barCursor, err := rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := barCursor.WriteValue(NewBytesDataFromStringWithFormat("shortstr", "st")); err != nil {
					t.Fatal(err)
				}

				// the slot tag is BYTES because the byte array is > 8 bytes long including the format tag
				assertEqual(t, TagBytes, barCursor.GetSlot().Tag)
				count, err := barCursor.Count()
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, int64(8), count)

				// read bar
				readBarCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				barBytesObj, err := readBarCursor.ReadBytesObject(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "shortstr", string(barBytesObj.Value))
				assertEqual(t, "st", string(barBytesObj.FormatTag))

				// make sure that BYTES can be read with a reader
				barReader, err := barCursor.Reader()
				if err != nil {
					t.Fatal(err)
				}
				barValue := make([]byte, count)
				if err := barReader.ReadFully(barValue); err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "shortstr", string(barValue))
			}

			// shorts
			{
				lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				var slotData WriteableData
				if lastSlot != nil {
					slotData = *lastSlot
				}
				barCursor, err := rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := barCursor.WriteValue(NewBytesDataFromStringWithFormat("shorts", "st")); err != nil {
					t.Fatal(err)
				}

				// the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long including the format tag
				assertEqual(t, TagShortBytes, barCursor.GetSlot().Tag)
				count, err := barCursor.Count()
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, int64(6), count)

				// read bar
				readBarCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				barBytesObj, err := readBarCursor.ReadBytesObject(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "shorts", string(barBytesObj.Value))
				assertEqual(t, "st", string(barBytesObj.FormatTag))

				// make sure that SHORT_BYTES can be read with a reader
				barReader, err := barCursor.Reader()
				if err != nil {
					t.Fatal(err)
				}
				barValue := make([]byte, count)
				if err := barReader.ReadFully(barValue); err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "shorts", string(barValue))
			}

			// short
			{
				lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				var slotData WriteableData
				if lastSlot != nil {
					slotData = *lastSlot
				}
				barCursor, err := rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := barCursor.WriteValue(NewBytesDataFromStringWithFormat("short", "st")); err != nil {
					t.Fatal(err)
				}

				// the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long including the format tag
				assertEqual(t, TagShortBytes, barCursor.GetSlot().Tag)
				count, err := barCursor.Count()
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, int64(5), count)

				// read bar
				readBarCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				barBytesObj, err := readBarCursor.ReadBytesObject(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "short", string(barBytesObj.Value))
				assertEqual(t, "st", string(barBytesObj.FormatTag))

				// make sure that SHORT_BYTES can be read with a reader
				barReader, err := barCursor.Reader()
				if err != nil {
					t.Fatal(err)
				}
				barValue := make([]byte, count)
				if err := barReader.ReadFully(barValue); err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "short", string(barValue))
			}
		}

		// read foo into buffer
		{
			barCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			barBufferValue, err := barCursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "baz", string(barBufferValue))
		}

		// write bar and get a pointer to it
		{
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			barSlotCursor, err := rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: barKey}},
				WriteDataPart{Data: NewBytesDataFromString("bar")},
			})
			if err != nil {
				t.Fatal(err)
			}
			barSlot := barSlotCursor.GetSlot()

			// overwrite foo -> bar using the bar pointer
			lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			slotData = nil
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
				WriteDataPart{Data: barSlot},
			})
			if err != nil {
				t.Fatal(err)
			}
			barCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			barValue, err := barCursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "bar", string(barValue))

			// can still read the old value
			bazCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -2},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			bazValue, err := bazCursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "baz", string(bazValue))

			// key not found
			notFoundKey := db.digest([]byte("this doesn't exist"))
			notFoundCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -2},
				HashMapGetPart{Target: HashMapGetValue{Hash: notFoundKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if notFoundCursor != nil {
				t.Fatal("expected nil cursor")
			}

			// write key that conflicts with foo the first two bytes
			smallConflictKey := db.digest([]byte("small conflict"))
			smallConflictKey[len(smallConflictKey)-1] = fooKey[len(fooKey)-1]
			smallConflictKey[len(smallConflictKey)-2] = fooKey[len(fooKey)-2]
			lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			slotData = nil
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: smallConflictKey}},
				WriteDataPart{Data: NewBytesDataFromString("small")},
			})
			if err != nil {
				t.Fatal(err)
			}

			// write key that conflicts with foo the first four bytes
			conflictKey := db.digest([]byte("conflict"))
			conflictKey[len(conflictKey)-1] = fooKey[len(fooKey)-1]
			conflictKey[len(conflictKey)-2] = fooKey[len(fooKey)-2]
			conflictKey[len(conflictKey)-3] = fooKey[len(fooKey)-3]
			conflictKey[len(conflictKey)-4] = fooKey[len(fooKey)-4]
			lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			slotData = nil
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: conflictKey}},
				WriteDataPart{Data: NewBytesDataFromString("hello")},
			})
			if err != nil {
				t.Fatal(err)
			}

			// read conflicting key
			helloCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: conflictKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			helloValue, err := helloCursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "hello", string(helloValue))

			// we can still read foo
			barCursor2, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			barValue2, err := barCursor2.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "bar", string(barValue2))

			// overwrite conflicting key
			lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			slotData = nil
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: conflictKey}},
				WriteDataPart{Data: NewBytesDataFromString("goodbye")},
			})
			if err != nil {
				t.Fatal(err)
			}
			goodbyeCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: conflictKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			goodbyeValue, err := goodbyeCursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "goodbye", string(goodbyeValue))

			// we can still read the old conflicting key
			helloCursor2, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -2},
				HashMapGetPart{Target: HashMapGetValue{Hash: conflictKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			helloValue2, err := helloCursor2.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "hello", string(helloValue2))

			// remove the conflicting keys
			{
				// foo's slot is an INDEX slot due to the conflict
				{
					mapCursor, err := rootCursor.ReadPath([]PathPart{
						ArrayListGet{Index: -1},
					})
					if err != nil {
						t.Fatal(err)
					}
					indexPos := mapCursor.GetSlot().Value
					assertEqual(t, TagHashMap, mapCursor.GetSlot().Tag)

					i := new(big.Int).SetBytes(fooKey)
					i.And(i, BigMask)
					idx := i.Int64()
					slotPos := indexPos + (int64(SlotLength) * idx)
					if err := core.SeekTo(slotPos); err != nil {
						t.Fatal(err)
					}
					var slotBytes [SlotLength]byte
					if err := core.Read(slotBytes[:]); err != nil {
						t.Fatal(err)
					}
					slot := SlotFromBytes(slotBytes)

					assertEqual(t, TagIndex, slot.Tag)
				}

				// remove the small conflict key
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapRemovePart{Hash: smallConflictKey},
				})
				if err != nil {
					t.Fatal(err)
				}

				// the conflict key still exists in history
				scCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -2},
					HashMapGetPart{Target: HashMapGetValue{Hash: smallConflictKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if scCursor == nil {
					t.Fatal("expected non-nil cursor")
				}

				// the conflict key doesn't exist in the latest moment
				scCursor2, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: smallConflictKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if scCursor2 != nil {
					t.Fatal("expected nil cursor")
				}

				// the other conflict key still exists
				ckCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: conflictKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if ckCursor == nil {
					t.Fatal("expected non-nil cursor")
				}

				// foo's slot is still an INDEX slot due to the other conflicting key
				{
					mapCursor, err := rootCursor.ReadPath([]PathPart{
						ArrayListGet{Index: -1},
					})
					if err != nil {
						t.Fatal(err)
					}
					indexPos := mapCursor.GetSlot().Value
					assertEqual(t, TagHashMap, mapCursor.GetSlot().Tag)

					i := new(big.Int).SetBytes(fooKey)
					i.And(i, BigMask)
					idx := i.Int64()
					slotPos := indexPos + (int64(SlotLength) * idx)
					if err := core.SeekTo(slotPos); err != nil {
						t.Fatal(err)
					}
					var slotBytes [SlotLength]byte
					if err := core.Read(slotBytes[:]); err != nil {
						t.Fatal(err)
					}
					slot := SlotFromBytes(slotBytes)

					assertEqual(t, TagIndex, slot.Tag)
				}

				// remove the conflict key
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapRemovePart{Hash: conflictKey},
				})
				if err != nil {
					t.Fatal(err)
				}

				// the conflict keys don't exist in the latest moment
				scCursor3, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: smallConflictKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if scCursor3 != nil {
					t.Fatal("expected nil cursor")
				}
				ckCursor2, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: conflictKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if ckCursor2 != nil {
					t.Fatal("expected nil cursor")
				}

				// foo's slot is now a KV_PAIR slot, because the branch was shortened
				{
					mapCursor, err := rootCursor.ReadPath([]PathPart{
						ArrayListGet{Index: -1},
					})
					if err != nil {
						t.Fatal(err)
					}
					indexPos := mapCursor.GetSlot().Value
					assertEqual(t, TagHashMap, mapCursor.GetSlot().Tag)

					i := new(big.Int).SetBytes(fooKey)
					i.And(i, BigMask)
					idx := i.Int64()
					slotPos := indexPos + (int64(SlotLength) * idx)
					if err := core.SeekTo(slotPos); err != nil {
						t.Fatal(err)
					}
					var slotBytes [SlotLength]byte
					if err := core.Read(slotBytes[:]); err != nil {
						t.Fatal(err)
					}
					slot := SlotFromBytes(slotBytes)

					assertEqual(t, TagKVPair, slot.Tag)
				}
			}

			{
				// overwrite foo with a uint
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
					WriteDataPart{Data: UintData{Value: 42}},
				})
				if err != nil {
					t.Fatal(err)
				}

				// read foo
				uintCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				uintValue, err := uintCursor.ReadUint()
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, uint64(42), uintValue)
			}

			{
				// overwrite foo with a int
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
					WriteDataPart{Data: IntData{Value: -42}},
				})
				if err != nil {
					t.Fatal(err)
				}

				// read foo
				intCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				intValue, err := intCursor.ReadInt()
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, int64(-42), intValue)
			}

			{
				// overwrite foo with a float
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
					WriteDataPart{Data: FloatData{Value: 42.5}},
				})
				if err != nil {
					t.Fatal(err)
				}

				// read foo
				floatCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
				})
				if err != nil {
					t.Fatal(err)
				}
				floatValue, err := floatCursor.ReadFloat()
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, 42.5, floatValue)
			}

			// remove foo
			lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			slotData = nil
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapRemovePart{Hash: fooKey},
			})
			if err != nil {
				t.Fatal(err)
			}

			// remove key that does not exist
			lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			slotData = nil
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, removeErr := rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapRemovePart{Hash: db.digest([]byte("doesn't exist"))},
			})
			if !errors.Is(removeErr, ErrKeyNotFound) {
				t.Fatalf("expected ErrKeyNotFound, got %v", removeErr)
			}

			// make sure foo doesn't exist anymore
			fooCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if fooCursor != nil {
				t.Fatal("expected nil cursor")
			}

			// non-top-level list
			{
				// write apple
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: NewBytesDataFromString("apple")},
				})
				if err != nil {
					t.Fatal(err)
				}

				// read apple
				appleCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListGet{Index: -1},
				})
				if err != nil {
					t.Fatal(err)
				}
				appleValue, err := appleCursor.ReadBytes(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "apple", string(appleValue))

				// write banana
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: NewBytesDataFromString("banana")},
				})
				if err != nil {
					t.Fatal(err)
				}

				// read banana
				bananaCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListGet{Index: -1},
				})
				if err != nil {
					t.Fatal(err)
				}
				bananaValue, err := bananaCursor.ReadBytes(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "banana", string(bananaValue))

				// can't read banana in older array_list
				noCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -2},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListGet{Index: 1},
				})
				if err != nil {
					t.Fatal(err)
				}
				if noCursor != nil {
					t.Fatal("expected nil cursor")
				}

				// write pear
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: NewBytesDataFromString("pear")},
				})
				if err != nil {
					t.Fatal(err)
				}

				// write grape
				lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
				if err != nil {
					t.Fatal(err)
				}
				slotData = nil
				if lastSlot != nil {
					slotData = *lastSlot
				}
				_, err = rootCursor.WritePath([]PathPart{
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: slotData},
					HashMapInitPart{},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListInit{},
					ArrayListAppend{},
					WriteDataPart{Data: NewBytesDataFromString("grape")},
				})
				if err != nil {
					t.Fatal(err)
				}

				// read pear
				pearCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListGet{Index: -2},
				})
				if err != nil {
					t.Fatal(err)
				}
				pearValue, err := pearCursor.ReadBytes(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "pear", string(pearValue))

				// read grape
				grapeCursor, err := rootCursor.ReadPath([]PathPart{
					ArrayListGet{Index: -1},
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("fruits"))}},
					ArrayListGet{Index: -1},
				})
				if err != nil {
					t.Fatal(err)
				}
				grapeValue, err := grapeCursor.ReadBytes(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "grape", string(grapeValue))
			}
		}
	}

	// append to top-level array_list many times, filling up the array_list until a root overflow occurs
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		watKey := db.digest([]byte("wat"))
		for i := 0; i < SlotCount+1; i++ {
			value := "wat" + strconv.Itoa(i)
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: watKey}},
				WriteDataPart{Data: NewBytesDataFromString(value)},
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < SlotCount+1; i++ {
			value := "wat" + strconv.Itoa(i)
			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: int64(i)},
				HashMapGetPart{Target: HashMapGetValue{Hash: watKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			value2, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, value, string(value2))
		}

		// add more slots to cause a new index block to be created.
		for i := SlotCount + 1; i < SlotCount*2+1; i++ {
			value := "wat" + strconv.Itoa(i)
			index := i

			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, writeErr := rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: watKey}},
				WriteDataPart{Data: NewBytesDataFromString(value)},
				ContextPart{Function: func(cursor *WriteCursor) error {
					if index == 32 {
						return errors.New("intentional error")
					}
					return nil
				}},
			})
			_ = writeErr
		}

		// try another append to make sure we still can.
		lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		var slotData WriteableData
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			HashMapInitPart{},
			HashMapGetPart{Target: HashMapGetValue{Hash: watKey}},
			WriteDataPart{Data: NewBytesDataFromString("wat32")},
		})
		if err != nil {
			t.Fatal(err)
		}

		// slice so it contains exactly SLOT_COUNT
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListSlice{Size: int64(SlotCount)},
		})
		if err != nil {
			t.Fatal(err)
		}

		// we can iterate over the remaining slots
		for i := 0; i < SlotCount; i++ {
			value := "wat" + strconv.Itoa(i)
			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: int64(i)},
				HashMapGetPart{Target: HashMapGetValue{Hash: watKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			value2, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, value, string(value2))
		}

		// but we can't get the value that we sliced out of the array list
		nilCursor, err := rootCursor.ReadPath([]PathPart{
			ArrayListGet{Index: int64(SlotCount + 1)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if nilCursor != nil {
			t.Fatal("expected nil cursor")
		}
	}

	// append to inner array_list many times, filling up the array_list until a root overflow occurs
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		for i := 0; i < SlotCount+1; i++ {
			value := "wat" + strconv.Itoa(i)
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: NewBytesDataFromString(value)},
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < SlotCount+1; i++ {
			value := "wat" + strconv.Itoa(i)
			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				ArrayListGet{Index: int64(i)},
			})
			if err != nil {
				t.Fatal(err)
			}
			value2, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, value, string(value2))
		}

		// slice the inner array list so it contains exactly SLOT_COUNT
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListGet{Index: -1},
			ArrayListInit{},
			ArrayListSlice{Size: int64(SlotCount)},
		})
		if err != nil {
			t.Fatal(err)
		}

		// we can iterate over the remaining slots
		for i := 0; i < SlotCount; i++ {
			value := "wat" + strconv.Itoa(i)
			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				ArrayListGet{Index: int64(i)},
			})
			if err != nil {
				t.Fatal(err)
			}
			value2, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, value, string(value2))
		}

		// but we can't get the value that we sliced out of the array list
		nilCursor, err := rootCursor.ReadPath([]PathPart{
			ArrayListGet{Index: -1},
			ArrayListGet{Index: int64(SlotCount + 1)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if nilCursor != nil {
			t.Fatal("expected nil cursor")
		}

		// overwrite the last value with hello
		lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		var slotData WriteableData
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			ArrayListInit{},
			ArrayListGet{Index: -1},
			WriteDataPart{Data: NewBytesDataFromString("hello")},
		})
		if err != nil {
			t.Fatal(err)
		}

		// read last value
		{
			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				ArrayListGet{Index: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			value, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "hello", string(value))
		}

		// overwrite the last value with goodbye
		lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		slotData = nil
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			ArrayListInit{},
			ArrayListGet{Index: -1},
			WriteDataPart{Data: NewBytesDataFromString("goodbye")},
		})
		if err != nil {
			t.Fatal(err)
		}

		// read last value
		{
			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				ArrayListGet{Index: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			value, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "goodbye", string(value))
		}

		// previous last value is still hello
		{
			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -2},
				ArrayListGet{Index: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			value, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "hello", string(value))
		}
	}

	// iterate over inner array_list
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		_, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		// add wats
		for i := 0; i < 10; i++ {
			value := "wat" + strconv.Itoa(i)
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: NewBytesDataFromString(value)},
			})
			if err != nil {
				t.Fatal(err)
			}

			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				ArrayListGet{Index: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			value2, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, value, string(value2))
		}

		// iterate over array_list
		{
			innerCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			i := 0
			for nextCursor, err := range innerCursor.All() {
				if err != nil {
					t.Fatal(err)
				}
				value := "wat" + strconv.Itoa(i)
				value2, err := nextCursor.ReadBytes(ptrInt64(1024))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, value, string(value2))
				i++
			}
			assertEqual(t, 10, i)
		}

		// set first slot to .none and make sure iteration still works
		{
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListGet{Index: -1},
				ArrayListInit{},
				ArrayListGet{Index: 0},
				WriteDataPart{Data: nil},
			})
			if err != nil {
				t.Fatal(err)
			}
			innerCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			i := 0
			for _, err := range innerCursor.All() {
				if err != nil {
					t.Fatal(err)
				}
				i++
			}
			assertEqual(t, 10, i)
		}

		// get list slot
		listCursor, err := rootCursor.ReadPath([]PathPart{
			ArrayListGet{Index: -1},
		})
		if err != nil {
			t.Fatal(err)
		}
		listCount, err := listCursor.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(10), listCount)
	}

	// iterate over inner hash_map
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		// add wats
		for i := 0; i < 10; i++ {
			value := "wat" + strconv.Itoa(i)
			watKey := db.digest([]byte(value))
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				HashMapInitPart{},
				HashMapGetPart{Target: HashMapGetValue{Hash: watKey}},
				WriteDataPart{Data: NewBytesDataFromString(value)},
			})
			if err != nil {
				t.Fatal(err)
			}

			cursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
				HashMapGetPart{Target: HashMapGetValue{Hash: watKey}},
			})
			if err != nil {
				t.Fatal(err)
			}
			value2, err := cursor.ReadBytes(ptrInt64(1024))
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, value, string(value2))
		}

		// add foo
		fooKey := db.digest([]byte("foo"))
		lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		var slotData WriteableData
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			HashMapInitPart{},
			HashMapGetPart{Target: HashMapGetKey{Hash: fooKey}},
			WriteDataPart{Data: NewBytesDataFromString("foo")},
		})
		if err != nil {
			t.Fatal(err)
		}
		lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		slotData = nil
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			HashMapInitPart{},
			HashMapGetPart{Target: HashMapGetValue{Hash: fooKey}},
			WriteDataPart{Data: UintData{Value: 42}},
		})
		if err != nil {
			t.Fatal(err)
		}

		// remove a wat
		lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		slotData = nil
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			HashMapInitPart{},
			HashMapRemovePart{Hash: db.digest([]byte("wat0"))},
		})
		if err != nil {
			t.Fatal(err)
		}

		// iterate over hash_map
		{
			innerCursor, err := rootCursor.ReadPath([]PathPart{
				ArrayListGet{Index: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			i := 0
			for kvPairCursor, err := range innerCursor.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Equal(kvPair.Hash, fooKey) {
					key, err := kvPair.KeyCursor.ReadBytes(ptrInt64(1024))
					if err != nil {
						t.Fatal(err)
					}
					assertEqual(t, "foo", string(key))
					assertEqual(t, int64(42), kvPair.ValueCursor.SlotPtr.Slot.Value)
				} else {
					value, err := kvPair.ValueCursor.ReadBytes(ptrInt64(1024))
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(kvPair.Hash, db.digest(value)) {
						t.Fatal("hash mismatch")
					}
				}
				i++
			}
			assertEqual(t, 10, i)
		}

		// iterate over hash_map with writeable cursor
		{
			lastSlot, err = rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			slotData = nil
			if lastSlot != nil {
				slotData = *lastSlot
			}
			innerCursor, err := rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
			})
			if err != nil {
				t.Fatal(err)
			}
			i := 0
			for kvPairCursor, err := range innerCursor.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Equal(kvPair.Hash, fooKey) {
					if err := kvPair.KeyCursor.WriteValue(NewBytesDataFromString("bar")); err != nil {
						t.Fatal(err)
					}
				}
				i++
			}
			assertEqual(t, 10, i)
		}
	}

	{
		// slice linked_array_list
		testSlice(t, core, hasher, SlotCount*5+1, 10, 5)
		testSlice(t, core, hasher, SlotCount*5+1, 0, int64(SlotCount*2))
		testSlice(t, core, hasher, SlotCount*5, int64(SlotCount*3), int64(SlotCount))
		testSlice(t, core, hasher, SlotCount*5, int64(SlotCount*3), int64(SlotCount*2))
		testSlice(t, core, hasher, SlotCount*2, 10, int64(SlotCount))
		testSlice(t, core, hasher, 2, 0, 2)
		testSlice(t, core, hasher, 2, 1, 1)
		testSlice(t, core, hasher, 1, 0, 0)

		// concat linked_array_list
		testConcat(t, core, hasher, int64(SlotCount*5+1), int64(SlotCount+1))
		testConcat(t, core, hasher, int64(SlotCount), int64(SlotCount))
		testConcat(t, core, hasher, 1, 1)
		testConcat(t, core, hasher, 0, 0)

		// insert linked_array_list
		testInsertAndRemove(t, core, hasher, 1, 0)
		testInsertAndRemove(t, core, hasher, 10, 0)
		testInsertAndRemove(t, core, hasher, 10, 5)
		testInsertAndRemove(t, core, hasher, 10, 9)
		testInsertAndRemove(t, core, hasher, SlotCount*5, int64(SlotCount*2))
	}

	// concat linked_array_list multiple times
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		var slotData WriteableData
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			HashMapInitPart{},
			ContextPart{Function: func(cursor *WriteCursor) error {
				values := make([]int64, 0)

				// create list
				for i := 0; i < SlotCount+1; i++ {
					n := int64(i) * 2
					values = append(values, n)
					_, err := cursor.WritePath([]PathPart{
						HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
						LinkedArrayListInit{},
						LinkedArrayListAppend{},
						WriteDataPart{Data: UintData{Value: uint64(n)}},
					})
					if err != nil {
						return err
					}
				}

				// get list slot
				evenListCursor, err := cursor.ReadPath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
				})
				if err != nil {
					return err
				}
				evenCount, err := evenListCursor.Count()
				if err != nil {
					return err
				}
				if evenCount != int64(SlotCount+1) {
					t.Fatalf("expected %d, got %d", SlotCount+1, evenCount)
				}

				// check all values in the new slice with an iterator
				{
					innerCursor, err := cursor.ReadPath([]PathPart{
						HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("even"))}},
					})
					if err != nil {
						return err
					}
					i := 0
					for _, err := range innerCursor.All() {
						if err != nil {
							return err
						}
						i++
					}
					if i != SlotCount+1 {
						t.Fatalf("expected %d, got %d", SlotCount+1, i)
					}
				}

				// concat the list with itself multiple times
				comboListCursor, err := cursor.WritePath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
					WriteDataPart{Data: evenListCursor.SlotPtr.Slot},
					LinkedArrayListInit{},
				})
				if err != nil {
					return err
				}
				for i := 0; i < 16; i++ {
					comboListCursor, err = comboListCursor.WritePath([]PathPart{
						LinkedArrayListConcatPart{List: evenListCursor.SlotPtr.Slot},
					})
					if err != nil {
						return err
					}
				}

				// append to the new list
				_, err = cursor.WritePath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
					LinkedArrayListAppend{},
					WriteDataPart{Data: UintData{Value: 3}},
				})
				if err != nil {
					return err
				}

				// read the new value from the list
				rc, err := cursor.ReadPath([]PathPart{
					HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
					LinkedArrayListGet{Index: -1},
				})
				if err != nil {
					return err
				}
				if rc.SlotPtr.Slot.Value != 3 {
					t.Fatalf("expected 3, got %d", rc.SlotPtr.Slot.Value)
				}

				// append more to the new list
				for i := 0; i < 500; i++ {
					_, err := cursor.WritePath([]PathPart{
						HashMapGetPart{Target: HashMapGetValue{Hash: db.digest([]byte("combo"))}},
						LinkedArrayListAppend{},
						WriteDataPart{Data: UintData{Value: 1}},
					})
					if err != nil {
						return err
					}
				}

				return nil
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// append items to linked_array_list without setting their value
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		_, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		// appending without setting any value should work
		for i := 0; i < 8; i++ {
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				LinkedArrayListInit{},
				LinkedArrayListAppend{},
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// explicitly writing a null slot should also work
		for i := 0; i < 8; i++ {
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				LinkedArrayListInit{},
				LinkedArrayListAppend{},
				WriteDataPart{Data: nil},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// insert at beginning of linked_array_list many times
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		var slotData WriteableData
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			LinkedArrayListInit{},
			LinkedArrayListAppend{},
			WriteDataPart{Data: UintData{Value: 42}},
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 1000; i++ {
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				LinkedArrayListInit{},
				LinkedArrayListInsertPart{Index: 0},
				WriteDataPart{Data: UintData{Value: uint64(i)}},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// insert at end of linked_array_list many times
	{
		if err := core.SetLength(0); err != nil {
			t.Fatal(err)
		}
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		rootCursor := db.RootCursor()

		lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
		if err != nil {
			t.Fatal(err)
		}
		var slotData WriteableData
		if lastSlot != nil {
			slotData = *lastSlot
		}
		_, err = rootCursor.WritePath([]PathPart{
			ArrayListInit{},
			ArrayListAppend{},
			WriteDataPart{Data: slotData},
			LinkedArrayListInit{},
			LinkedArrayListAppend{},
			WriteDataPart{Data: UintData{Value: 42}},
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 1000; i++ {
			lastSlot, err := rootCursor.ReadPathSlot([]PathPart{ArrayListGet{Index: -1}})
			if err != nil {
				t.Fatal(err)
			}
			var slotData WriteableData
			if lastSlot != nil {
				slotData = *lastSlot
			}
			_, err = rootCursor.WritePath([]PathPart{
				ArrayListInit{},
				ArrayListAppend{},
				WriteDataPart{Data: slotData},
				LinkedArrayListInit{},
				LinkedArrayListInsertPart{Index: int64(i)},
				WriteDataPart{Data: UintData{Value: uint64(i)}},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
