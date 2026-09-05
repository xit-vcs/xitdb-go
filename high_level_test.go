package xitdb

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"io"
	"iter"
	"os"
	"strconv"
	"testing"
)

func sha1Hasher() Hasher {
	return Hasher{
		Hash: sha1.New(),
	}
}

func sha1HasherWithID() Hasher {
	return Hasher{
		Hash: sha1.New(),
		ID:   BytesToID([4]byte{'s', 'h', 'a', '1'}),
	}
}

func TestHighLevelApi(t *testing.T) {
	// CoreMemory
	{
		core := NewCoreMemory()
		hasher := sha1Hasher()
		testHighLevelApi(t, core, hasher, nil)
	}

	// CoreFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())

		core := NewCoreFile(f)
		defer core.Close()
		hasher := sha1Hasher()
		testHighLevelApi(t, core, hasher, f)
	}

	// CoreBufferedFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())

		core := NewCoreBufferedFileWithSize(f, 1024)
		defer core.Close()
		hasher := sha1Hasher()
		testHighLevelApi(t, core, hasher, f)
	}
}

func TestNotUsingArrayListAtTopLevel(t *testing.T) {
	// hash map
	{
		core := NewCoreMemory()
		hasher := sha1Hasher()
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}

		m, err := NewWriteHashMap(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Put("foo", NewString("foo")); err != nil {
			t.Fatal(err)
		}
		if err := m.Put("bar", NewString("bar")); err != nil {
			t.Fatal(err)
		}

		// init inner map
		{
			innerMapCursor, err := m.PutCursor("inner-map")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewWriteHashMap(innerMapCursor); err != nil {
				t.Fatal(err)
			}
		}

		// re-init inner map
		{
			innerMapCursor, err := m.PutCursor("inner-map")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewWriteHashMap(innerMapCursor); err != nil {
				t.Fatal(err)
			}
		}
	}

	// linked array list is not currently allowed at the top level
	{
		core := NewCoreMemory()
		hasher := sha1Hasher()
		db, err := NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewWriteLinkedArrayList(db.RootCursor())
		if err == nil {
			t.Fatal("expected error for linked array list at top level")
		}
	}
}

func TestReadDatabaseFromResources(t *testing.T) {
	f, err := os.Open("testdata/test.db")
	if err != nil {
		t.Fatal(err)
	}

	core := NewCoreFile(f)
	defer core.Close()
	hasher := sha1Hasher()
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}

	history, err := NewReadArrayList(db.RootCursor().ReadCursor)
	if err != nil {
		t.Fatal(err)
	}

	// moment 0
	{
		momentCursor, err := history.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		fooCursor, err := moment.GetCursor("foo")
		if err != nil {
			t.Fatal(err)
		}
		fooValue, err := fooCursor.ReadBytes(int64(1024))
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "foo", string(fooValue))

		fooSlot, err := moment.GetSlot("foo")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, fooSlot.Tag)
		barSlot, err := moment.GetSlot("bar")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, barSlot.Tag)

		fruitsCursor, err := moment.GetCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruits, err := NewReadArrayList(fruitsCursor)
		if err != nil {
			t.Fatal(err)
		}
		fruitsCount, err := fruits.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), fruitsCount)

		appleCursor, err := fruits.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		appleValue, err := appleCursor.ReadBytes(int64(1024))
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "apple", string(appleValue))

		peopleCursor, err := moment.GetCursor("people")
		if err != nil {
			t.Fatal(err)
		}
		people, err := NewReadArrayList(peopleCursor)
		if err != nil {
			t.Fatal(err)
		}
		peopleCount, err := people.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), peopleCount)

		aliceCursor, err := people.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		alice, err := NewReadHashMap(aliceCursor)
		if err != nil {
			t.Fatal(err)
		}
		aliceAgeCursor, err := alice.GetCursor("age")
		if err != nil {
			t.Fatal(err)
		}
		aliceAge, err := aliceAgeCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(25), aliceAge)

		todosCursor, err := moment.GetCursor("todos")
		if err != nil {
			t.Fatal(err)
		}
		todos, err := NewReadLinkedArrayList(todosCursor)
		if err != nil {
			t.Fatal(err)
		}
		todosCount, err := todos.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), todosCount)

		todoCursor, err := todos.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		todoValue, err := todoCursor.ReadBytes(int64(1024))
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "Pay the bills", string(todoValue))

		// iterate over people -> person -> person fields
		for personCursor, err := range people.All() {
			if err != nil {
				t.Fatal(err)
			}
			person, err := NewReadHashMap(personCursor)
			if err != nil {
				t.Fatal(err)
			}
			for kvPairCursor, err := range person.All() {
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		// counted hash map
		{
			lcmCursor, err := moment.GetCursor("letters-counted-map")
			if err != nil {
				t.Fatal(err)
			}
			lcm, err := NewReadCountedHashMap(lcmCursor)
			if err != nil {
				t.Fatal(err)
			}
			lcmCount, err := lcm.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(2), lcmCount)

			count := 0
			for kvPairCursor, err := range lcm.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPair.KeyCursor.ReadBytes(int64(1024))
				if err != nil {
					t.Fatal(err)
				}
				count++
			}
			assertEqual(t, 2, count)
		}

		// hash set
		{
			lsCursor, err := moment.GetCursor("letters-set")
			if err != nil {
				t.Fatal(err)
			}
			ls, err := NewReadHashSet(lsCursor)
			if err != nil {
				t.Fatal(err)
			}
			aCursor, err := ls.GetCursor("a")
			if err != nil {
				t.Fatal(err)
			}
			if aCursor == nil {
				t.Fatal("expected non-nil cursor for 'a'")
			}
			cCursor, err := ls.GetCursor("c")
			if err != nil {
				t.Fatal(err)
			}
			if cCursor == nil {
				t.Fatal("expected non-nil cursor for 'c'")
			}

			count := 0
			for kvPairCursor, err := range ls.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPair.KeyCursor.ReadBytes(int64(1024))
				if err != nil {
					t.Fatal(err)
				}
				count++
			}
			assertEqual(t, 2, count)
		}

		// counted hash set
		{
			lcsCursor, err := moment.GetCursor("letters-counted-set")
			if err != nil {
				t.Fatal(err)
			}
			lcs, err := NewReadCountedHashSet(lcsCursor)
			if err != nil {
				t.Fatal(err)
			}
			lcsCount, err := lcs.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(2), lcsCount)

			count := 0
			for kvPairCursor, err := range lcs.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPair.KeyCursor.ReadBytes(int64(1024))
				if err != nil {
					t.Fatal(err)
				}
				count++
			}
			assertEqual(t, 2, count)
		}
	}

	// moment 1
	{
		momentCursor, err := history.GetCursor(1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		barCursor, err := moment.GetCursor("bar")
		if err != nil {
			t.Fatal(err)
		}
		if barCursor != nil {
			t.Fatal("expected nil cursor for 'bar'")
		}

		fruitsKeyCursor, err := moment.GetKeyCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruitsKeyValue, err := fruitsKeyCursor.ReadBytes(int64(1024))
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "fruits", string(fruitsKeyValue))

		fruitsCursor, err := moment.GetCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruits, err := NewReadArrayList(fruitsCursor)
		if err != nil {
			t.Fatal(err)
		}
		fruitsCount, err := fruits.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), fruitsCount)

		fruitsKV, err := moment.GetKeyValuePair("fruits")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, fruitsKV.KeyCursor.SlotPtr.Slot.Tag)
		assertEqual(t, TagArrayList, fruitsKV.ValueCursor.SlotPtr.Slot.Tag)

		lemonCursor, err := fruits.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		lemonValue, err := lemonCursor.ReadBytes(int64(1024))
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "lemon", string(lemonValue))

		peopleCursor, err := moment.GetCursor("people")
		if err != nil {
			t.Fatal(err)
		}
		people, err := NewReadArrayList(peopleCursor)
		if err != nil {
			t.Fatal(err)
		}
		peopleCount, err := people.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), peopleCount)

		aliceCursor, err := people.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		alice, err := NewReadHashMap(aliceCursor)
		if err != nil {
			t.Fatal(err)
		}
		aliceAgeCursor, err := alice.GetCursor("age")
		if err != nil {
			t.Fatal(err)
		}
		aliceAge, err := aliceAgeCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(26), aliceAge)

		todosCursor, err := moment.GetCursor("todos")
		if err != nil {
			t.Fatal(err)
		}
		todos, err := NewReadLinkedArrayList(todosCursor)
		if err != nil {
			t.Fatal(err)
		}
		todosCount, err := todos.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), todosCount)

		todoCursor, err := todos.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		todoValue, err := todoCursor.ReadBytes(int64(1024))
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "Wash the car", string(todoValue))

		lcmCursor, err := moment.GetCursor("letters-counted-map")
		if err != nil {
			t.Fatal(err)
		}
		lcm, err := NewReadCountedHashMap(lcmCursor)
		if err != nil {
			t.Fatal(err)
		}
		lcmCount, err := lcm.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), lcmCount)

		lsCursor, err := moment.GetCursor("letters-set")
		if err != nil {
			t.Fatal(err)
		}
		ls, err := NewReadHashSet(lsCursor)
		if err != nil {
			t.Fatal(err)
		}
		aCursor, err := ls.GetCursor("a")
		if err != nil {
			t.Fatal(err)
		}
		if aCursor == nil {
			t.Fatal("expected non-nil cursor for 'a'")
		}
		cCursor, err := ls.GetCursor("c")
		if err != nil {
			t.Fatal(err)
		}
		if cCursor != nil {
			t.Fatal("expected nil cursor for 'c'")
		}

		lcsCursor, err := moment.GetCursor("letters-counted-set")
		if err != nil {
			t.Fatal(err)
		}
		lcs, err := NewReadCountedHashSet(lcsCursor)
		if err != nil {
			t.Fatal(err)
		}
		lcsCount, err := lcs.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), lcsCount)
	}
}

func TestMultithreading(t *testing.T) {
	f, err := os.Open("testdata/test.db")
	if err != nil {
		t.Fatal(err)
	}

	core := NewCoreFile(f)
	defer core.Close()
	hasher := sha1Hasher()
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}

	history, err := NewReadArrayList(db.RootCursor().ReadCursor)
	if err != nil {
		t.Fatal(err)
	}

	// read from the main goroutine to move the read position
	momentCursor, err := history.GetCursor(0)
	if err != nil {
		t.Fatal(err)
	}
	moment, err := NewReadHashMap(momentCursor)
	if err != nil {
		t.Fatal(err)
	}

	readFoo := func(t *testing.T) {
		// each goroutine opens its own file handle
		f2, err := os.Open("testdata/test.db")
		if err != nil {
			t.Error(err)
			return
		}

		core2 := NewCoreFile(f2)
		defer core2.Close()
		db2, err := NewDatabase(core2, hasher)
		if err != nil {
			t.Error(err)
			return
		}
		history2, err := NewReadArrayList(db2.RootCursor().ReadCursor)
		if err != nil {
			t.Error(err)
			return
		}
		mc, err := history2.GetCursor(0)
		if err != nil {
			t.Error(err)
			return
		}
		m, err := NewReadHashMap(mc)
		if err != nil {
			t.Error(err)
			return
		}
		fooCursor, err := m.GetCursor("foo")
		if err != nil {
			t.Error(err)
			return
		}
		fooValue, err := fooCursor.ReadBytes(int64(1024))
		if err != nil {
			t.Error(err)
			return
		}
		if string(fooValue) != "foo" {
			t.Errorf("expected foo, got %s", string(fooValue))
		}
	}

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		readFoo(t)
		close(done1)
	}()
	go func() {
		readFoo(t)
		close(done2)
	}()

	// this should succeed because the goroutines use their own file handles
	fooCursor, err := moment.GetCursor("foo")
	if err != nil {
		t.Fatal(err)
	}
	fooValue, err := fooCursor.ReadBytes(int64(1024))
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, "foo", string(fooValue))

	<-done1
	<-done2
}

func testHighLevelApi(t *testing.T, core Core, hasher Hasher, fileMaybe *os.File) {
	t.Helper()
	maxRead := int64(1024)

	// init the db
	if err := core.SetLength(0); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}

	// first transaction
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}

			if err := moment.Put("foo", NewString("foo")); err != nil {
				return err
			}
			if err := moment.Put("bar", NewString("bar")); err != nil {
				return err
			}

			fruitsCursor, err := moment.PutCursor("fruits")
			if err != nil {
				return err
			}
			fruits, err := NewWriteArrayList(fruitsCursor)
			if err != nil {
				return err
			}
			if err := fruits.Append(NewString("apple")); err != nil {
				return err
			}
			if err := fruits.Append(NewString("pear")); err != nil {
				return err
			}
			if err := fruits.Append(NewString("grape")); err != nil {
				return err
			}

			peopleCursor, err := moment.PutCursor("people")
			if err != nil {
				return err
			}
			people, err := NewWriteArrayList(peopleCursor)
			if err != nil {
				return err
			}

			aliceCursor, err := people.AppendCursor()
			if err != nil {
				return err
			}
			alice, err := NewWriteHashMap(aliceCursor)
			if err != nil {
				return err
			}
			if err := alice.Put("name", NewString("Alice")); err != nil {
				return err
			}
			if err := alice.Put("age", NewUint(25)); err != nil {
				return err
			}

			bobCursor, err := people.AppendCursor()
			if err != nil {
				return err
			}
			bob, err := NewWriteHashMap(bobCursor)
			if err != nil {
				return err
			}
			if err := bob.Put("name", NewString("Bob")); err != nil {
				return err
			}
			if err := bob.Put("age", NewUint(42)); err != nil {
				return err
			}

			todosCursor, err := moment.PutCursor("todos")
			if err != nil {
				return err
			}
			todos, err := NewWriteLinkedArrayList(todosCursor)
			if err != nil {
				return err
			}
			if err := todos.Append(NewString("Pay the bills")); err != nil {
				return err
			}
			if err := todos.Append(NewString("Get an oil change")); err != nil {
				return err
			}
			if err := todos.Insert(1, NewString("Wash the car")); err != nil {
				return err
			}

			// make sure insertCursor works
			todoCursor, err := todos.InsertCursor(1)
			if err != nil {
				return err
			}
			if _, err := NewWriteHashMap(todoCursor); err != nil {
				return err
			}
			if err := todos.Remove(1); err != nil {
				return err
			}

			lcmCursor, err := moment.PutCursor("letters-counted-map")
			if err != nil {
				return err
			}
			lcm, err := NewWriteCountedHashMap(lcmCursor)
			if err != nil {
				return err
			}
			if err := lcm.Put("a", NewUint(1)); err != nil {
				return err
			}
			if err := lcm.Put("a", NewUint(2)); err != nil {
				return err
			}
			if err := lcm.Put("c", NewUint(2)); err != nil {
				return err
			}

			lsCursor, err := moment.PutCursor("letters-set")
			if err != nil {
				return err
			}
			ls, err := NewWriteHashSet(lsCursor)
			if err != nil {
				return err
			}
			if err := ls.Put("a"); err != nil {
				return err
			}
			if err := ls.Put("a"); err != nil {
				return err
			}
			if err := ls.Put("c"); err != nil {
				return err
			}

			lcsCursor, err := moment.PutCursor("letters-counted-set")
			if err != nil {
				return err
			}
			lcs, err := NewWriteCountedHashSet(lcsCursor)
			if err != nil {
				return err
			}
			if err := lcs.Put("a"); err != nil {
				return err
			}
			if err := lcs.Put("a"); err != nil {
				return err
			}
			if err := lcs.Put("c"); err != nil {
				return err
			}

			randomBytes := bytes.Repeat([]byte{0xAB}, 32)
			if err := moment.Put("random-number", NewTaggedBytes(randomBytes, []byte("bi"))); err != nil {
				return err
			}

			longTextCursor, err := moment.PutCursor("long-text")
			if err != nil {
				return err
			}
			cursorWriter, err := longTextCursor.Writer()
			if err != nil {
				return err
			}
			bw := bufio.NewWriter(cursorWriter)
			for i := 0; i < 50; i++ {
				if _, err := bw.Write([]byte("hello, world\n")); err != nil {
					return err
				}
			}
			if err := bw.Flush(); err != nil {
				return err
			}
			if err := cursorWriter.Finish(); err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify moment 0
		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		fooCursor, err := moment.GetCursor("foo")
		if err != nil {
			t.Fatal(err)
		}
		fooValue, err := fooCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "foo", string(fooValue))

		fooSlot, err := moment.GetSlot("foo")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, fooSlot.Tag)
		barSlot, err := moment.GetSlot("bar")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, barSlot.Tag)

		fruitsCursor, err := moment.GetCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruits, err := NewReadArrayList(fruitsCursor)
		if err != nil {
			t.Fatal(err)
		}
		fruitsCount, err := fruits.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), fruitsCount)

		appleCursor, err := fruits.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		appleValue, err := appleCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "apple", string(appleValue))

		peopleCursor, err := moment.GetCursor("people")
		if err != nil {
			t.Fatal(err)
		}
		people, err := NewReadArrayList(peopleCursor)
		if err != nil {
			t.Fatal(err)
		}
		peopleCount, err := people.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), peopleCount)

		aliceCursor, err := people.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		alice, err := NewReadHashMap(aliceCursor)
		if err != nil {
			t.Fatal(err)
		}
		aliceAgeCursor, err := alice.GetCursor("age")
		if err != nil {
			t.Fatal(err)
		}
		aliceAge, err := aliceAgeCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(25), aliceAge)

		todosCursor, err := moment.GetCursor("todos")
		if err != nil {
			t.Fatal(err)
		}
		todos, err := NewReadLinkedArrayList(todosCursor)
		if err != nil {
			t.Fatal(err)
		}
		todosCount, err := todos.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), todosCount)

		todoCursor, err := todos.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		todoValue, err := todoCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "Pay the bills", string(todoValue))

		// iterate over people
		for personCursor, err := range people.All() {
			if err != nil {
				t.Fatal(err)
			}
			person, err := NewReadHashMap(personCursor)
			if err != nil {
				t.Fatal(err)
			}
			for kvPairCursor, err := range person.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPair.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					t.Fatal(err)
				}
				switch kvPair.ValueCursor.SlotPtr.Slot.Tag {
				case TagShortBytes, TagBytes:
					_, err = kvPair.ValueCursor.ReadBytes(maxRead)
				case TagUint:
					_, err = kvPair.ValueCursor.ReadUint()
				case TagInt:
					_, err = kvPair.ValueCursor.ReadInt()
				case TagFloat:
					_, err = kvPair.ValueCursor.ReadFloat()
				default:
					t.Fatalf("unexpected tag: %d", kvPair.ValueCursor.SlotPtr.Slot.Tag)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		// iterate over fruits
		for _, err := range fruits.All() {
			if err != nil {
				t.Fatal(err)
			}
		}

		// counted hash map
		{
			lcmCursor, err := moment.GetCursor("letters-counted-map")
			if err != nil {
				t.Fatal(err)
			}
			lcm, err := NewReadCountedHashMap(lcmCursor)
			if err != nil {
				t.Fatal(err)
			}
			lcmCount, err := lcm.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(2), lcmCount)

			count := 0
			for kvPairCursor, err := range lcm.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPair.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					t.Fatal(err)
				}
				count++
			}
			assertEqual(t, 2, count)
		}

		// hash set
		{
			lsCursor, err := moment.GetCursor("letters-set")
			if err != nil {
				t.Fatal(err)
			}
			ls, err := NewReadHashSet(lsCursor)
			if err != nil {
				t.Fatal(err)
			}
			aCursor, err := ls.GetCursor("a")
			if err != nil {
				t.Fatal(err)
			}
			if aCursor == nil {
				t.Fatal("expected non-nil cursor for 'a'")
			}
			cCursor, err := ls.GetCursor("c")
			if err != nil {
				t.Fatal(err)
			}
			if cCursor == nil {
				t.Fatal("expected non-nil cursor for 'c'")
			}

			count := 0
			for kvPairCursor, err := range ls.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPair.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					t.Fatal(err)
				}
				count++
			}
			assertEqual(t, 2, count)
		}

		// counted hash set
		{
			lcsCursor, err := moment.GetCursor("letters-counted-set")
			if err != nil {
				t.Fatal(err)
			}
			lcs, err := NewReadCountedHashSet(lcsCursor)
			if err != nil {
				t.Fatal(err)
			}
			lcsCount, err := lcs.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(2), lcsCount)

			count := 0
			for kvPairCursor, err := range lcs.All() {
				if err != nil {
					t.Fatal(err)
				}
				kvPair, err := kvPairCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}
				_, err = kvPair.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					t.Fatal(err)
				}
				count++
			}
			assertEqual(t, 2, count)
		}

		// random number format tag
		{
			rnCursor, err := moment.GetCursor("random-number")
			if err != nil {
				t.Fatal(err)
			}
			rnObj, err := rnCursor.ReadBytesObject(maxRead)
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "bi", string(rnObj.FormatTag))
		}

		// long text with reader
		{
			ltCursor, err := moment.GetCursor("long-text")
			if err != nil {
				t.Fatal(err)
			}
			cursorReader, err := ltCursor.Reader()
			if err != nil {
				t.Fatal(err)
			}
			br := bufio.NewReader(cursorReader)
			count := 0
			for {
				_, err := br.ReadString('\n')
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				count++
			}
			assertEqual(t, 50, count)
		}
	}

	// second transaction: modify data
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}

			removed, err := moment.Remove("bar")
			if err != nil {
				return err
			}
			if !removed {
				t.Fatal("expected bar to be removed")
			}
			removed, err = moment.Remove("doesn't exist")
			if err != nil {
				return err
			}
			if removed {
				t.Fatal("expected not found")
			}

			fruitsCursor, err := moment.PutCursor("fruits")
			if err != nil {
				return err
			}
			fruits, err := NewWriteArrayList(fruitsCursor)
			if err != nil {
				return err
			}
			if err := fruits.Put(0, NewString("lemon")); err != nil {
				return err
			}
			if err := fruits.Slice(2); err != nil {
				return err
			}

			peopleCursor, err := moment.PutCursor("people")
			if err != nil {
				return err
			}
			people, err := NewWriteArrayList(peopleCursor)
			if err != nil {
				return err
			}

			aliceCursor, err := people.PutCursor(0)
			if err != nil {
				return err
			}
			alice, err := NewWriteHashMap(aliceCursor)
			if err != nil {
				return err
			}
			if err := alice.Put("age", NewUint(26)); err != nil {
				return err
			}

			todosCursor, err := moment.PutCursor("todos")
			if err != nil {
				return err
			}
			todos, err := NewWriteLinkedArrayList(todosCursor)
			if err != nil {
				return err
			}
			if err := todos.Concat(todosCursor.Slot()); err != nil {
				return err
			}
			if err := todos.Slice(1, 2); err != nil {
				return err
			}
			if err := todos.Remove(1); err != nil {
				return err
			}

			lcmCursor, err := moment.PutCursor("letters-counted-map")
			if err != nil {
				return err
			}
			lcm, err := NewWriteCountedHashMap(lcmCursor)
			if err != nil {
				return err
			}
			lcm.Remove("b")
			lcm.Remove("c")

			lsCursor, err := moment.PutCursor("letters-set")
			if err != nil {
				return err
			}
			ls, err := NewWriteHashSet(lsCursor)
			if err != nil {
				return err
			}
			ls.Remove("b")
			ls.Remove("c")

			lcsCursor, err := moment.PutCursor("letters-counted-set")
			if err != nil {
				return err
			}
			lcs, err := NewWriteCountedHashSet(lcsCursor)
			if err != nil {
				return err
			}
			lcs.Remove("b")
			lcs.Remove("c")

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify moment 1
		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		barCursor, err := moment.GetCursor("bar")
		if err != nil {
			t.Fatal(err)
		}
		if barCursor != nil {
			t.Fatal("expected nil cursor for 'bar'")
		}

		fruitsKeyCursor, err := moment.GetKeyCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruitsKeyValue, err := fruitsKeyCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "fruits", string(fruitsKeyValue))

		fruitsCursor, err := moment.GetCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruits, err := NewReadArrayList(fruitsCursor)
		if err != nil {
			t.Fatal(err)
		}
		fruitsCount, err := fruits.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), fruitsCount)

		fruitsKV, err := moment.GetKeyValuePair("fruits")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, fruitsKV.KeyCursor.SlotPtr.Slot.Tag)
		assertEqual(t, TagArrayList, fruitsKV.ValueCursor.SlotPtr.Slot.Tag)

		lemonCursor, err := fruits.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		lemonValue, err := lemonCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "lemon", string(lemonValue))

		peopleCursor, err := moment.GetCursor("people")
		if err != nil {
			t.Fatal(err)
		}
		people, err := NewReadArrayList(peopleCursor)
		if err != nil {
			t.Fatal(err)
		}
		peopleCount, err := people.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), peopleCount)

		aliceCursor, err := people.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		alice, err := NewReadHashMap(aliceCursor)
		if err != nil {
			t.Fatal(err)
		}
		aliceAgeCursor, err := alice.GetCursor("age")
		if err != nil {
			t.Fatal(err)
		}
		aliceAge, err := aliceAgeCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(26), aliceAge)

		todosCursor, err := moment.GetCursor("todos")
		if err != nil {
			t.Fatal(err)
		}
		todos, err := NewReadLinkedArrayList(todosCursor)
		if err != nil {
			t.Fatal(err)
		}
		todosCount, err := todos.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), todosCount)

		todoCursor, err := todos.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		todoValue, err := todoCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "Wash the car", string(todoValue))

		lcmCursor, err := moment.GetCursor("letters-counted-map")
		if err != nil {
			t.Fatal(err)
		}
		lcm, err := NewReadCountedHashMap(lcmCursor)
		if err != nil {
			t.Fatal(err)
		}
		lcmCount, err := lcm.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), lcmCount)

		lsCursor, err := moment.GetCursor("letters-set")
		if err != nil {
			t.Fatal(err)
		}
		ls, err := NewReadHashSet(lsCursor)
		if err != nil {
			t.Fatal(err)
		}
		aCursor, err := ls.GetCursor("a")
		if err != nil {
			t.Fatal(err)
		}
		if aCursor == nil {
			t.Fatal("expected non-nil cursor for 'a'")
		}
		cCursor, err := ls.GetCursor("c")
		if err != nil {
			t.Fatal(err)
		}
		if cCursor != nil {
			t.Fatal("expected nil cursor for 'c'")
		}

		lcsCursor, err := moment.GetCursor("letters-counted-set")
		if err != nil {
			t.Fatal(err)
		}
		lcs, err := NewReadCountedHashSet(lcsCursor)
		if err != nil {
			t.Fatal(err)
		}
		lcsCount, err := lcs.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), lcsCount)
	}

	// old data hasn't changed
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		momentCursor, err := history.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		fooCursor, err := moment.GetCursor("foo")
		if err != nil {
			t.Fatal(err)
		}
		fooValue, err := fooCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "foo", string(fooValue))

		fooSlot, err := moment.GetSlot("foo")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, fooSlot.Tag)
		barSlot, err := moment.GetSlot("bar")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagShortBytes, barSlot.Tag)

		fruitsCursor, err := moment.GetCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruits, err := NewReadArrayList(fruitsCursor)
		if err != nil {
			t.Fatal(err)
		}
		fruitsCount, err := fruits.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), fruitsCount)

		appleCursor, err := fruits.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		appleValue, err := appleCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "apple", string(appleValue))

		peopleCursor, err := moment.GetCursor("people")
		if err != nil {
			t.Fatal(err)
		}
		people, err := NewReadArrayList(peopleCursor)
		if err != nil {
			t.Fatal(err)
		}
		peopleCount, err := people.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), peopleCount)

		aliceCursor, err := people.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		alice, err := NewReadHashMap(aliceCursor)
		if err != nil {
			t.Fatal(err)
		}
		aliceAgeCursor, err := alice.GetCursor("age")
		if err != nil {
			t.Fatal(err)
		}
		aliceAge, err := aliceAgeCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(25), aliceAge)

		todosCursor, err := moment.GetCursor("todos")
		if err != nil {
			t.Fatal(err)
		}
		todos, err := NewReadLinkedArrayList(todosCursor)
		if err != nil {
			t.Fatal(err)
		}
		todosCount, err := todos.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), todosCount)

		todoCursor, err := todos.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		todoValue, err := todoCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "Pay the bills", string(todoValue))
	}

	// remove the last transaction with slice
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		if err := history.Slice(1); err != nil {
			t.Fatal(err)
		}

		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		fooCursor, err := moment.GetCursor("foo")
		if err != nil {
			t.Fatal(err)
		}
		fooValue, err := fooCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "foo", string(fooValue))
	}

	// opening the db leaves trailing data alone, because it may
	// belong to another writer's unfinished transaction.
	{
		coreLen, err := core.Length()
		if err != nil {
			t.Fatal(err)
		}
		if err := core.SeekTo(coreLen); err != nil {
			t.Fatal(err)
		}

		if err := core.Write([]byte("this is trailing data from an unfinished transaction")); err != nil {
			t.Fatal(err)
		}
		if err := core.Flush(); err != nil {
			t.Fatal(err)
		}
		sizeWithTail, err := core.Length()
		if err != nil {
			t.Fatal(err)
		}

		// no error is thrown if db file is opened in read-only mode
		if fileMaybe != nil {
			readOnlyFile, err := os.Open(fileMaybe.Name())
			if err != nil {
				t.Fatal(err)
			}
			readOnlyCore := NewCoreFile(readOnlyFile)
			defer readOnlyCore.Close()
			_, err = NewDatabase(readOnlyCore, hasher)
			if err != nil {
				t.Fatal(err)
			}
		}

		db, err = NewDatabase(core, hasher)
		if err != nil {
			t.Fatal(err)
		}

		sizeAfter, err := core.Length()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, sizeWithTail, sizeAfter)
	}

	// cloning
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}

			fruitsCursor, err := moment.GetCursor("fruits")
			if err != nil {
				return err
			}
			fruits, err := NewReadArrayList(fruitsCursor)
			if err != nil {
				return err
			}

			foodCursor, err := moment.PutCursor("food")
			if err != nil {
				return err
			}
			if err := foodCursor.Write(fruits.Slot()); err != nil {
				return err
			}

			food, err := NewWriteArrayList(foodCursor)
			if err != nil {
				return err
			}
			if err := food.Append(NewString("eggs")); err != nil {
				return err
			}
			if err := food.Append(NewString("rice")); err != nil {
				return err
			}
			if err := food.Append(NewString("fish")); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		foodCursor, err := moment.GetCursor("food")
		if err != nil {
			t.Fatal(err)
		}
		food, err := NewReadArrayList(foodCursor)
		if err != nil {
			t.Fatal(err)
		}
		foodCount, err := food.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(6), foodCount)

		fruitsCursor, err := moment.GetCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruits, err := NewReadArrayList(fruitsCursor)
		if err != nil {
			t.Fatal(err)
		}
		fruitsCount, err := fruits.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), fruitsCount)
	}

	// accidental mutation when cloning inside a transaction
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		historyCount, err := history.Count()
		if err != nil {
			t.Fatal(err)
		}
		historyIndex := historyCount - 1

		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}

			bigCitiesCursor, err := moment.PutCursor("big-cities")
			if err != nil {
				return err
			}
			bigCities, err := NewWriteArrayList(bigCitiesCursor)
			if err != nil {
				return err
			}
			if err := bigCities.Append(NewString("New York, NY")); err != nil {
				return err
			}
			if err := bigCities.Append(NewString("Los Angeles, CA")); err != nil {
				return err
			}

			citiesCursor, err := moment.PutCursor("cities")
			if err != nil {
				return err
			}
			if err := citiesCursor.Write(bigCities.Slot()); err != nil {
				return err
			}

			cities, err := NewWriteArrayList(citiesCursor)
			if err != nil {
				return err
			}
			if err := cities.Append(NewString("Charleston, SC")); err != nil {
				return err
			}
			if err := cities.Append(NewString("Louisville, KY")); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		citiesCursor, err := moment.GetCursor("cities")
		if err != nil {
			t.Fatal(err)
		}
		cities, err := NewReadArrayList(citiesCursor)
		if err != nil {
			t.Fatal(err)
		}
		citiesCount, err := cities.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(4), citiesCount)

		// big-cities also got mutated (accidental)
		bigCitiesCursor, err := moment.GetCursor("big-cities")
		if err != nil {
			t.Fatal(err)
		}
		bigCities, err := NewReadArrayList(bigCitiesCursor)
		if err != nil {
			t.Fatal(err)
		}
		bigCitiesCount, err := bigCities.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(4), bigCitiesCount)

		// revert
		histSlot, err := history.GetSlot(historyIndex)
		if err != nil {
			t.Fatal(err)
		}
		if err := history.Append(histSlot); err != nil {
			t.Fatal(err)
		}
	}

	// preventing accidental mutation with freezing
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}

			bigCitiesCursor, err := moment.PutCursor("big-cities")
			if err != nil {
				return err
			}
			bigCities, err := NewWriteArrayList(bigCitiesCursor)
			if err != nil {
				return err
			}
			if err := bigCities.Append(NewString("New York, NY")); err != nil {
				return err
			}
			if err := bigCities.Append(NewString("Los Angeles, CA")); err != nil {
				return err
			}

			// freeze here, so big-cities won't be mutated
			if err := cursor.DB.Freeze(); err != nil {
				return err
			}

			citiesCursor, err := moment.PutCursor("cities")
			if err != nil {
				return err
			}
			if err := citiesCursor.Write(bigCities.Slot()); err != nil {
				return err
			}

			cities, err := NewWriteArrayList(citiesCursor)
			if err != nil {
				return err
			}
			if err := cities.Append(NewString("Charleston, SC")); err != nil {
				return err
			}
			if err := cities.Append(NewString("Louisville, KY")); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		citiesCursor, err := moment.GetCursor("cities")
		if err != nil {
			t.Fatal(err)
		}
		cities, err := NewReadArrayList(citiesCursor)
		if err != nil {
			t.Fatal(err)
		}
		citiesCount, err := cities.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(4), citiesCount)

		bigCitiesCursor, err := moment.GetCursor("big-cities")
		if err != nil {
			t.Fatal(err)
		}
		bigCities, err := NewReadArrayList(bigCitiesCursor)
		if err != nil {
			t.Fatal(err)
		}
		bigCitiesCount, err := bigCities.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), bigCitiesCount)
	}

	// build a secondary index with a SortedMap to sort and paginate,
	// like the "Sorting and Paginating" section of the readme
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}

		type user struct {
			id       string
			username string
			name     string
		}

		// inserted in arbitrary order; the index sorts them alphabetically
		newUsers := []user{
			{id: "user000000000001", username: "dave", name: "Dave Smith"},
			{id: "user000000000002", username: "alice", name: "Alice Jones"},
			{id: "user000000000003", username: "carol", name: "Carol White"},
			{id: "user000000000004", username: "dan", name: "Dan Brown"},
			{id: "user000000000005", username: "bob", name: "Bob Lee"},
			{id: "user000000000006", username: "eve", name: "Eve Adams"},
		}

		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}

			// the primary store: a HashMap from user id to the user's fields
			idToUserCursor, err := moment.PutCursor("id->user")
			if err != nil {
				return err
			}
			idToUser, err := NewWriteHashMap(idToUserCursor)
			if err != nil {
				return err
			}

			// the secondary index: a SortedMap ordered alphabetically by username
			usernameToIDCursor, err := moment.PutCursor("username->id")
			if err != nil {
				return err
			}
			usernameToID, err := NewWriteSortedMap(usernameToIDCursor)
			if err != nil {
				return err
			}

			for _, u := range newUsers {
				userCursor, err := idToUser.PutCursor(u.id)
				if err != nil {
					return err
				}
				userMap, err := NewWriteHashMap(userCursor)
				if err != nil {
					return err
				}
				if err := userMap.Put("username", NewString(u.username)); err != nil {
					return err
				}
				if err := userMap.Put("name", NewString(u.name)); err != nil {
					return err
				}

				// the key is the username (the sort key); the value is the id
				if err := usernameToID.Put(u.username, NewString(u.id)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		idToUserCursor, err := moment.GetCursor("id->user")
		if err != nil {
			t.Fatal(err)
		}
		idToUser, err := NewReadHashMap(idToUserCursor)
		if err != nil {
			t.Fatal(err)
		}

		usernameToIDCursor, err := moment.GetCursor("username->id")
		if err != nil {
			t.Fatal(err)
		}
		usernameToID, err := NewReadSortedMap(usernameToIDCursor)
		if err != nil {
			t.Fatal(err)
		}

		count, err := usernameToID.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(len(newUsers)), count)

		// page through the index two at a time and check we get every user back
		// in alphabetical order by username (not the order they were inserted)
		pageSize := int64(2)
		expectedNames := []string{"Alice Jones", "Bob Lee", "Carol White", "Dan Brown", "Dave Smith", "Eve Adams"}

		seen := 0
		for after := int64(0); after < count; after += pageSize {
			end := after + pageSize
			if end > count {
				end = count
			}
			// seek straight to the start of the page, then walk forward
			i := after
			for idCursor, err := range usernameToID.AllFromIndex(after) {
				if err != nil {
					t.Fatal(err)
				}
				if i >= end {
					break
				}

				idKv, err := idCursor.ReadKeyValuePair()
				if err != nil {
					t.Fatal(err)
				}

				// the index entry's value is the user id; use it to read the
				// full user out of the primary map
				userIDBytes, err := idKv.ValueCursor.ReadBytes(maxRead)
				if err != nil {
					t.Fatal(err)
				}
				userID := string(userIDBytes)

				userCursor, err := idToUser.GetCursor(userID)
				if err != nil {
					t.Fatal(err)
				}
				userMap, err := NewReadHashMap(userCursor)
				if err != nil {
					t.Fatal(err)
				}
				nameCursor, err := userMap.GetCursor("name")
				if err != nil {
					t.Fatal(err)
				}
				nameBytes, err := nameCursor.ReadBytes(maxRead)
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, expectedNames[seen], string(nameBytes))
				seen += 1
				i += 1
			}
		}
		assertEqual(t, len(newUsers), seen)

		// autocomplete: seek straight to the first username >= "da", then walk
		// forward only while the prefix matches. this lower-bound seek by key is
		// the thing an ArrayList can't do.
		prefix := []byte("da")
		expectedMatches := []string{"dan", "dave"}
		matches := 0
		for idCursor, err := range usernameToID.AllFrom(prefix) {
			if err != nil {
				t.Fatal(err)
			}

			idKv, err := idCursor.ReadKeyValuePair()
			if err != nil {
				t.Fatal(err)
			}

			// the key is the username; stop once we've walked past the prefix
			usernameBytes, err := idKv.KeyCursor.ReadBytes(maxRead)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(usernameBytes, prefix) {
				break
			}

			assertEqual(t, expectedMatches[matches], string(usernameBytes))
			matches += 1
		}
		assertEqual(t, len(expectedMatches), matches)
	}
}

func TestCompaction(t *testing.T) {
	maxRead := int64(1024)

	// memory
	{
		sourceCore := NewCoreMemory()
		targetCore := NewCoreMemory()
		hasher := sha1Hasher()
		testCompaction(t, sourceCore, targetCore, hasher, false, maxRead)
	}

	// file
	{
		sf, err := os.CreateTemp("", "compact_source")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(sf.Name())
		tf, err := os.CreateTemp("", "compact_target")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tf.Name())

		sourceCore := NewCoreFile(sf)
		targetCore := NewCoreFile(tf)
		defer sourceCore.Close()
		defer targetCore.Close()
		hasher := sha1Hasher()
		testCompaction(t, sourceCore, targetCore, hasher, true, maxRead)
	}

	// buffered file
	{
		sf, err := os.CreateTemp("", "compact_source")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(sf.Name())
		tf, err := os.CreateTemp("", "compact_target")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tf.Name())

		sourceCore := NewCoreBufferedFile(sf)
		targetCore := NewCoreBufferedFile(tf)
		defer sourceCore.Close()
		defer targetCore.Close()
		hasher := sha1Hasher()
		testCompaction(t, sourceCore, targetCore, hasher, true, maxRead)
	}
}

func testCompaction(t *testing.T, sourceCore, targetCore Core, hasher Hasher, isFile bool, maxRead int64) {
	t.Helper()

	// empty DB compaction
	{
		mustSetLength(t, sourceCore, 0)
		mustSetLength(t, targetCore, 0)
		source, err := NewDatabase(sourceCore, hasher)
		if err != nil {
			t.Fatal(err)
		}
		compacted, err := source.Compact(targetCore)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, TagNone, compacted.Header.Tag)
	}

	// basic compaction with various data types
	{
		mustSetLength(t, sourceCore, 0)
		mustSetLength(t, targetCore, 0)
		source, err := NewDatabase(sourceCore, hasher)
		if err != nil {
			t.Fatal(err)
		}

		// moment 1
		{
			history, err := NewWriteArrayList(source.RootCursor())
			if err != nil {
				t.Fatal(err)
			}
			lastSlot, err := history.GetSlot(-1)
			if err != nil {
				t.Fatal(err)
			}
			err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
				moment, err := NewWriteHashMap(cursor)
				if err != nil {
					return err
				}
				if err := moment.Put("key1", NewString("value1")); err != nil {
					return err
				}
				if err := moment.Put("key2", NewUint(100)); err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// moment 2
		{
			history, err := NewWriteArrayList(source.RootCursor())
			if err != nil {
				t.Fatal(err)
			}
			lastSlot, err := history.GetSlot(-1)
			if err != nil {
				t.Fatal(err)
			}
			err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
				moment, err := NewWriteHashMap(cursor)
				if err != nil {
					return err
				}
				if err := moment.Put("key1", NewString("updated_value1")); err != nil {
					return err
				}
				if err := moment.Put("key2", NewUint(200)); err != nil {
					return err
				}
				if err := moment.Put("key3", NewInt(-42)); err != nil {
					return err
				}
				if err := moment.Put("key4", NewFloat(3.14)); err != nil {
					return err
				}
				if err := moment.Put("short", NewString("hi")); err != nil {
					return err
				}
				if err := moment.Put("tagged", NewTaggedString("this is a long tagged string!!", "bi")); err != nil {
					return err
				}

				fruitsCursor, err := moment.PutCursor("fruits")
				if err != nil {
					return err
				}
				fruits, err := NewWriteArrayList(fruitsCursor)
				if err != nil {
					return err
				}
				if err := fruits.Append(NewString("apple")); err != nil {
					return err
				}
				if err := fruits.Append(NewString("banana")); err != nil {
					return err
				}
				if err := fruits.Append(NewString("cherry")); err != nil {
					return err
				}

				todosCursor, err := moment.PutCursor("todos")
				if err != nil {
					return err
				}
				todos, err := NewWriteLinkedArrayList(todosCursor)
				if err != nil {
					return err
				}
				if err := todos.Append(NewString("task1")); err != nil {
					return err
				}
				if err := todos.Append(NewString("task2")); err != nil {
					return err
				}
				if err := todos.Append(NewString("task3")); err != nil {
					return err
				}

				countedCursor, err := moment.PutCursor("counted")
				if err != nil {
					return err
				}
				counted, err := NewWriteCountedHashMap(countedCursor)
				if err != nil {
					return err
				}
				if err := counted.Put("a", NewUint(1)); err != nil {
					return err
				}
				if err := counted.PutKey("a", NewString("a")); err != nil {
					return err
				}
				if err := counted.Put("b", NewUint(2)); err != nil {
					return err
				}
				if err := counted.PutKey("b", NewString("b")); err != nil {
					return err
				}

				setCursor, err := moment.PutCursor("myset")
				if err != nil {
					return err
				}
				set, err := NewWriteHashSet(setCursor)
				if err != nil {
					return err
				}
				if err := set.Put("x"); err != nil {
					return err
				}
				if err := set.Put("y"); err != nil {
					return err
				}

				csetCursor, err := moment.PutCursor("mycset")
				if err != nil {
					return err
				}
				cset, err := NewWriteCountedHashSet(csetCursor)
				if err != nil {
					return err
				}
				if err := cset.Put("p"); err != nil {
					return err
				}
				if err := cset.Put("q"); err != nil {
					return err
				}

				// SortedMap
				sortedCursor, err := moment.PutCursor("sorted")
				if err != nil {
					return err
				}
				sorted, err := NewWriteSortedMap(sortedCursor)
				if err != nil {
					return err
				}
				if err := sorted.Put("apple", NewUint(1)); err != nil {
					return err
				}
				if err := sorted.Put("banana", NewUint(2)); err != nil {
					return err
				}
				if err := sorted.Put("cherry", NewUint(3)); err != nil {
					return err
				}

				// SortedSet
				sortedSetCursor, err := moment.PutCursor("sortedset")
				if err != nil {
					return err
				}
				sortedSet, err := NewWriteSortedSet(sortedSetCursor)
				if err != nil {
					return err
				}
				if err := sortedSet.Put("foo"); err != nil {
					return err
				}
				if err := sortedSet.Put("bar"); err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// moment 3
		{
			history, err := NewWriteArrayList(source.RootCursor())
			if err != nil {
				t.Fatal(err)
			}
			lastSlot, err := history.GetSlot(-1)
			if err != nil {
				t.Fatal(err)
			}
			err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
				moment, err := NewWriteHashMap(cursor)
				if err != nil {
					return err
				}
				if err := moment.Put("key1", NewString("final_value")); err != nil {
					return err
				}

				// cycles must survive compaction rather than causing the
				// remapper to recurse indefinitely.
				if err := moment.Put("self", moment.Slot()); err != nil {
					return err
				}

				cyclicListCursor, err := moment.PutCursor("cyclic-list")
				if err != nil {
					return err
				}
				cyclicList, err := NewWriteArrayList(cyclicListCursor)
				if err != nil {
					return err
				}
				if err := cyclicList.Append(cyclicList.Slot()); err != nil {
					return err
				}

				mapACursor, err := moment.PutCursor("map-a")
				if err != nil {
					return err
				}
				mapA, err := NewWriteHashMap(mapACursor)
				if err != nil {
					return err
				}
				mapBCursor, err := moment.PutCursor("map-b")
				if err != nil {
					return err
				}
				mapB, err := NewWriteHashMap(mapBCursor)
				if err != nil {
					return err
				}
				if err := mapA.Put("map-b", mapB.Slot()); err != nil {
					return err
				}
				return mapB.Put("map-a", mapA.Slot())
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		sourceSize, err := sourceCore.Length()
		if err != nil {
			t.Fatal(err)
		}

		compacted, err := source.Compact(targetCore)
		if err != nil {
			t.Fatal(err)
		}

		targetSize, err := targetCore.Length()
		if err != nil {
			t.Fatal(err)
		}
		if targetSize >= sourceSize {
			t.Fatalf("target should be smaller: %d >= %d", targetSize, sourceSize)
		}

		history, err := NewReadArrayList(compacted.RootCursor().ReadCursor)
		if err != nil {
			t.Fatal(err)
		}
		hCount, err := history.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), hCount)

		momentCursor, err := history.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		// self-references and mutual references point back to the same compacted
		// objects rather than duplicate objects or dangling source offsets.
		selfCursor, err := moment.GetCursor("self")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, momentCursor.Slot(), selfCursor.Slot())

		cyclicListCursor, err := moment.GetCursor("cyclic-list")
		if err != nil {
			t.Fatal(err)
		}
		cyclicList, err := NewReadArrayList(cyclicListCursor)
		if err != nil {
			t.Fatal(err)
		}
		cyclicSlot, err := cyclicList.GetSlot(0)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, cyclicListCursor.Slot(), cyclicSlot)

		mapACursor, err := moment.GetCursor("map-a")
		if err != nil {
			t.Fatal(err)
		}
		mapA, err := NewReadHashMap(mapACursor)
		if err != nil {
			t.Fatal(err)
		}
		mapBCursor, err := mapA.GetCursor("map-b")
		if err != nil {
			t.Fatal(err)
		}
		mapB, err := NewReadHashMap(mapBCursor)
		if err != nil {
			t.Fatal(err)
		}
		mapASlot, err := mapB.GetSlot("map-a")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, mapACursor.Slot(), mapASlot)

		key1Cursor, err := moment.GetCursor("key1")
		if err != nil {
			t.Fatal(err)
		}
		key1Value, err := key1Cursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "final_value", string(key1Value))

		key2Cursor, err := moment.GetCursor("key2")
		if err != nil {
			t.Fatal(err)
		}
		key2Value, err := key2Cursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(200), key2Value)

		key3Cursor, err := moment.GetCursor("key3")
		if err != nil {
			t.Fatal(err)
		}
		key3Value, err := key3Cursor.ReadInt()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(-42), key3Value)

		key4Cursor, err := moment.GetCursor("key4")
		if err != nil {
			t.Fatal(err)
		}
		key4Value, err := key4Cursor.ReadFloat()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, 3.14, key4Value)

		shortCursor, err := moment.GetCursor("short")
		if err != nil {
			t.Fatal(err)
		}
		shortValue, err := shortCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "hi", string(shortValue))

		taggedCursor, err := moment.GetCursor("tagged")
		if err != nil {
			t.Fatal(err)
		}
		taggedObj, err := taggedCursor.ReadBytesObject(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "this is a long tagged string!!", string(taggedObj.Value))
		assertEqual(t, "bi", string(taggedObj.FormatTag))

		fruitsCursor, err := moment.GetCursor("fruits")
		if err != nil {
			t.Fatal(err)
		}
		fruits, err := NewReadArrayList(fruitsCursor)
		if err != nil {
			t.Fatal(err)
		}
		fCount, err := fruits.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), fCount)
		appleCursor, err := fruits.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		appleValue, err := appleCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "apple", string(appleValue))
		cherryCursor, err := fruits.GetCursor(2)
		if err != nil {
			t.Fatal(err)
		}
		cherryValue, err := cherryCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "cherry", string(cherryValue))

		todosCursor, err := moment.GetCursor("todos")
		if err != nil {
			t.Fatal(err)
		}
		todos, err := NewReadLinkedArrayList(todosCursor)
		if err != nil {
			t.Fatal(err)
		}
		tCount, err := todos.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), tCount)
		t1Cursor, err := todos.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		t1Value, err := t1Cursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "task1", string(t1Value))
		t3Cursor, err := todos.GetCursor(2)
		if err != nil {
			t.Fatal(err)
		}
		t3Value, err := t3Cursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "task3", string(t3Value))

		countedCursor, err := moment.GetCursor("counted")
		if err != nil {
			t.Fatal(err)
		}
		counted, err := NewReadCountedHashMap(countedCursor)
		if err != nil {
			t.Fatal(err)
		}
		cCount, err := counted.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), cCount)
		aCursor, err := counted.GetCursor("a")
		if err != nil {
			t.Fatal(err)
		}
		aValue, err := aCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(1), aValue)
		bCursor, err := counted.GetCursor("b")
		if err != nil {
			t.Fatal(err)
		}
		bValue, err := bCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(2), bValue)

		setCursor, err := moment.GetCursor("myset")
		if err != nil {
			t.Fatal(err)
		}
		set, err := NewReadHashSet(setCursor)
		if err != nil {
			t.Fatal(err)
		}
		xCursor, err := set.GetCursor("x")
		if err != nil {
			t.Fatal(err)
		}
		xValue, err := xCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "x", string(xValue))

		csetCursor, err := moment.GetCursor("mycset")
		if err != nil {
			t.Fatal(err)
		}
		cset, err := NewReadCountedHashSet(csetCursor)
		if err != nil {
			t.Fatal(err)
		}
		csCount, err := cset.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), csCount)
		pCursor, err := cset.GetCursor("p")
		if err != nil {
			t.Fatal(err)
		}
		pValue, err := pCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "p", string(pValue))

		// SortedMap
		sortedCursor, err := moment.GetCursor("sorted")
		if err != nil {
			t.Fatal(err)
		}
		sorted, err := NewReadSortedMap(sortedCursor)
		if err != nil {
			t.Fatal(err)
		}
		sortedCount, err := sorted.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(3), sortedCount)
		bananaCursor, err := sorted.GetCursor("banana")
		if err != nil {
			t.Fatal(err)
		}
		bananaValue, err := bananaCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(2), bananaValue)
		// lexicographic order is preserved across compaction
		firstKv, err := sorted.GetIndexKeyValuePair(0)
		if err != nil {
			t.Fatal(err)
		}
		firstKey, err := firstKv.KeyCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "apple", string(firstKey))
		lastKv, err := sorted.GetIndexKeyValuePair(-1)
		if err != nil {
			t.Fatal(err)
		}
		lastKey, err := lastKv.KeyCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "cherry", string(lastKey))

		// SortedSet
		sortedSetCursor, err := moment.GetCursor("sortedset")
		if err != nil {
			t.Fatal(err)
		}
		sortedSet, err := NewReadSortedSet(sortedSetCursor)
		if err != nil {
			t.Fatal(err)
		}
		sortedSetCount, err := sortedSet.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(2), sortedSetCount)
		hasFoo, err := sortedSet.Contains("foo")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, true, hasFoo)
		hasBar, err := sortedSet.Contains("bar")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, true, hasBar)
		hasBaz, err := sortedSet.Contains("baz")
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, false, hasBaz)
	}

	// structural sharing
	{
		mustSetLength(t, sourceCore, 0)
		mustSetLength(t, targetCore, 0)
		source, err := NewDatabase(sourceCore, hasher)
		if err != nil {
			t.Fatal(err)
		}

		// moment 1: create many keys
		{
			history, err := NewWriteArrayList(source.RootCursor())
			if err != nil {
				t.Fatal(err)
			}
			lastSlot, err := history.GetSlot(-1)
			if err != nil {
				t.Fatal(err)
			}
			err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
				moment, err := NewWriteHashMap(cursor)
				if err != nil {
					return err
				}
				for i := 0; i < 20; i++ {
					key := "shared_key_" + itoa(i)
					if err := moment.Put(key, NewUint(uint64(i))); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// moments 2-5: change only one key each time
		for round := 0; round < 4; round++ {
			history, err := NewWriteArrayList(source.RootCursor())
			if err != nil {
				t.Fatal(err)
			}
			lastSlot, err := history.GetSlot(-1)
			if err != nil {
				t.Fatal(err)
			}
			r := round
			err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
				moment, err := NewWriteHashMap(cursor)
				if err != nil {
					return err
				}
				return moment.Put("changing_key", NewUint(uint64(r+100)))
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		compacted, err := source.Compact(targetCore)
		if err != nil {
			t.Fatal(err)
		}

		history, err := NewReadArrayList(compacted.RootCursor().ReadCursor)
		if err != nil {
			t.Fatal(err)
		}
		hCount, err := history.Count()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, int64(1), hCount)

		momentCursor, err := history.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 20; i++ {
			key := "shared_key_" + itoa(i)
			cursor, err := moment.GetCursor(key)
			if err != nil {
				t.Fatal(err)
			}
			v, err := cursor.ReadUint()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, uint64(i), v)
		}

		changingCursor, err := moment.GetCursor("changing_key")
		if err != nil {
			t.Fatal(err)
		}
		changingValue, err := changingCursor.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(103), changingValue)
	}

	// re-open after compact and compact-then-continue-writing
	if isFile {
		// re-open after compact
		{
			mustSetLength(t, sourceCore, 0)
			mustSetLength(t, targetCore, 0)
			source, err := NewDatabase(sourceCore, hasher)
			if err != nil {
				t.Fatal(err)
			}

			history, err := NewWriteArrayList(source.RootCursor())
			if err != nil {
				t.Fatal(err)
			}
			lastSlot, err := history.GetSlot(-1)
			if err != nil {
				t.Fatal(err)
			}
			err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
				moment, err := NewWriteHashMap(cursor)
				if err != nil {
					return err
				}
				if err := moment.Put("persist", NewString("persistent_value")); err != nil {
					return err
				}
				return moment.Put("number", NewUint(999))
			})
			if err != nil {
				t.Fatal(err)
			}

			source.Compact(targetCore)

			if err := targetCore.SeekTo(0); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewDatabase(targetCore, hasher)
			if err != nil {
				t.Fatal(err)
			}

			rHistory, err := NewReadArrayList(reopened.RootCursor().ReadCursor)
			if err != nil {
				t.Fatal(err)
			}
			rCount, err := rHistory.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(1), rCount)

			mc, err := rHistory.GetCursor(0)
			if err != nil {
				t.Fatal(err)
			}
			m, err := NewReadHashMap(mc)
			if err != nil {
				t.Fatal(err)
			}
			pCursor, err := m.GetCursor("persist")
			if err != nil {
				t.Fatal(err)
			}
			pValue, err := pCursor.ReadBytes(maxRead)
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "persistent_value", string(pValue))
			nCursor, err := m.GetCursor("number")
			if err != nil {
				t.Fatal(err)
			}
			nValue, err := nCursor.ReadUint()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, uint64(999), nValue)
		}

		// compact then continue writing
		{
			mustSetLength(t, sourceCore, 0)
			mustSetLength(t, targetCore, 0)
			source, err := NewDatabase(sourceCore, hasher)
			if err != nil {
				t.Fatal(err)
			}

			history, err := NewWriteArrayList(source.RootCursor())
			if err != nil {
				t.Fatal(err)
			}
			lastSlot, err := history.GetSlot(-1)
			if err != nil {
				t.Fatal(err)
			}
			err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
				moment, err := NewWriteHashMap(cursor)
				if err != nil {
					return err
				}
				return moment.Put("original", NewString("original_data"))
			})
			if err != nil {
				t.Fatal(err)
			}

			compacted, err := source.Compact(targetCore)
			if err != nil {
				t.Fatal(err)
			}

			// add new moment to compacted DB
			{
				cHistory, err := NewWriteArrayList(compacted.RootCursor())
				if err != nil {
					t.Fatal(err)
				}
				cLastSlot, err := cHistory.GetSlot(-1)
				if err != nil {
					t.Fatal(err)
				}
				err = cHistory.AppendContext(cLastSlot, func(cursor *WriteCursor) error {
					moment, err := NewWriteHashMap(cursor)
					if err != nil {
						return err
					}
					return moment.Put("new_key", NewString("new_data"))
				})
				if err != nil {
					t.Fatal(err)
				}
			}

			cHistory, err := NewReadArrayList(compacted.RootCursor().ReadCursor)
			if err != nil {
				t.Fatal(err)
			}
			cCount, err := cHistory.Count()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, int64(2), cCount)

			m0Cursor, err := cHistory.GetCursor(0)
			if err != nil {
				t.Fatal(err)
			}
			m0, err := NewReadHashMap(m0Cursor)
			if err != nil {
				t.Fatal(err)
			}
			origCursor, err := m0.GetCursor("original")
			if err != nil {
				t.Fatal(err)
			}
			origValue, err := origCursor.ReadBytes(maxRead)
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "original_data", string(origValue))

			m1Cursor, err := cHistory.GetCursor(1)
			if err != nil {
				t.Fatal(err)
			}
			m1, err := NewReadHashMap(m1Cursor)
			if err != nil {
				t.Fatal(err)
			}
			newCursor, err := m1.GetCursor("new_key")
			if err != nil {
				t.Fatal(err)
			}
			newValue, err := newCursor.ReadBytes(maxRead)
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "new_data", string(newValue))

			origCursor2, err := m1.GetCursor("original")
			if err != nil {
				t.Fatal(err)
			}
			origValue2, err := origCursor2.ReadBytes(maxRead)
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "original_data", string(origValue2))
		}
	}
}

// helpers

func assertEqual[T comparable](t *testing.T, expected, actual T) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

func mustSetLength(t *testing.T, core Core, length int64) {
	t.Helper()
	if err := core.SetLength(length); err != nil {
		t.Fatal(err)
	}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func TestSortedMap(t *testing.T) {
	// CoreMemory
	{
		testSortedMap(t, NewCoreMemory(), sha1Hasher())
	}
	// CoreFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		core := NewCoreFile(f)
		defer core.Close()
		testSortedMap(t, core, sha1Hasher())
	}
	// CoreBufferedFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		core := NewCoreBufferedFileWithSize(f, 1024)
		defer core.Close()
		testSortedMap(t, core, sha1Hasher())
	}
}

func testSortedMap(t *testing.T, core Core, hasher Hasher) {
	t.Helper()
	maxRead := int64(1024)
	if err := core.SetLength(0); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}

	// keys "k0000".."k0059" sort lexicographically in numeric order
	const count = 60
	k := func(i int) string {
		s := strconv.Itoa(i)
		for len(s) < 4 {
			s = "0" + s
		}
		return "k" + s
	}

	// the first key yielded by a ranged iterator
	firstKey := func(seq iter.Seq2[*ReadCursor, error]) (string, error) {
		for c, err := range seq {
			if err != nil {
				return "", err
			}
			kv, err := c.ReadKeyValuePair()
			if err != nil {
				return "", err
			}
			b, err := kv.KeyCursor.ReadBytes(maxRead)
			if err != nil {
				return "", err
			}
			return string(b), nil
		}
		return "", nil
	}

	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}
		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}
			mapCursor, err := moment.PutCursor("map")
			if err != nil {
				return err
			}
			m, err := NewWriteSortedMap(mapCursor)
			if err != nil {
				return err
			}

			// insert in reverse order to exercise front-insertions and splits
			for i := count; i > 0; {
				i--
				if err := m.Put(k(i), NewUint(uint64(i))); err != nil {
					return err
				}
			}
			if c, err := m.Count(); err != nil {
				return err
			} else {
				assertEqual(t, int64(count), c)
			}

			// dedup: re-putting an existing key replaces the value, not the count
			if err := m.Put("k0005", NewUint(999)); err != nil {
				return err
			}
			if c, err := m.Count(); err != nil {
				return err
			} else {
				assertEqual(t, int64(count), c)
			}
			cur, err := m.GetCursor("k0005")
			if err != nil {
				return err
			}
			v, err := cur.ReadUint()
			if err != nil {
				return err
			}
			assertEqual(t, uint64(999), v)
			if err := m.Put("k0005", NewUint(5)); err != nil {
				return err
			}

			// ordered iteration yields k0000..k0059 with intact values
			n := 0
			for c, err := range m.All() {
				if err != nil {
					return err
				}
				kv, err := c.ReadKeyValuePair()
				if err != nil {
					return err
				}
				keyBytes, err := kv.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					return err
				}
				assertEqual(t, k(n), string(keyBytes))
				val, err := kv.ValueCursor.ReadUint()
				if err != nil {
					return err
				}
				assertEqual(t, uint64(n), val)
				n++
			}
			assertEqual(t, count, n)

			c42, err := m.GetCursor("k0042")
			if err != nil {
				return err
			}
			if c42 == nil {
				t.Fatal("k0042 should be present")
			}
			cNope, err := m.GetCursor("nope")
			if err != nil {
				return err
			}
			if cNope != nil {
				t.Fatal("nope should be absent")
			}

			// getByIndex (positive and negative) and rank are inverses
			for idx := 0; idx < count; idx++ {
				kv, err := m.GetIndexKeyValuePair(int64(idx))
				if err != nil {
					return err
				}
				keyBytes, err := kv.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					return err
				}
				assertEqual(t, k(idx), string(keyBytes))
				r, err := m.RankByBytes(keyBytes)
				if err != nil {
					return err
				}
				assertEqual(t, int64(idx), r)
			}
			lastKv, err := m.GetIndexKeyValuePair(-1)
			if err != nil {
				return err
			}
			lastKey, err := lastKv.KeyCursor.ReadBytes(maxRead)
			if err != nil {
				return err
			}
			assertEqual(t, "k0059", string(lastKey))
			outKv, err := m.GetIndexKeyValuePair(count)
			if err != nil {
				return err
			}
			if outKv != nil {
				t.Fatal("index == count should be absent")
			}

			// lower-bound iteration from a present and an absent key
			if s, err := firstKey(m.AllFrom([]byte("k0030"))); err != nil {
				return err
			} else {
				assertEqual(t, "k0030", s)
			}
			// "k00095" sorts between "k0009" and "k0010"
			if s, err := firstKey(m.AllFrom([]byte("k00095"))); err != nil {
				return err
			} else {
				assertEqual(t, "k0010", s)
			}
			if s, err := firstKey(m.AllFromIndex(count - 2)); err != nil {
				return err
			} else {
				assertEqual(t, "k0058", s)
			}
			// negative indexes count from the end: -1 is the last entry, -count the first
			if s, err := firstKey(m.AllFromIndex(-1)); err != nil {
				return err
			} else {
				assertEqual(t, "k0059", s)
			}
			{
				// -1 is the last entry, so the iterator yields exactly one
				n := 0
				for _, err := range m.AllFromIndex(-1) {
					if err != nil {
						return err
					}
					n++
				}
				assertEqual(t, 1, n)
			}
			if s, err := firstKey(m.AllFromIndex(-count)); err != nil {
				return err
			} else {
				assertEqual(t, "k0000", s)
			}
			{
				// out of range past either end yields nothing
				n := 0
				for _, err := range m.AllFromIndex(count) {
					if err != nil {
						return err
					}
					n++
				}
				for _, err := range m.AllFromIndex(-count - 1) {
					if err != nil {
						return err
					}
					n++
				}
				assertEqual(t, 0, n)
			}

			// remove the even keys, then re-verify order, count, and presence
			for j := 0; j < count; j += 2 {
				ok, err := m.Remove(k(j))
				if err != nil {
					return err
				}
				assertEqual(t, true, ok)
			}
			if c, err := m.Count(); err != nil {
				return err
			} else {
				assertEqual(t, int64(count/2), c)
			}
			gone, err := m.Remove("k0000")
			if err != nil {
				return err
			}
			assertEqual(t, false, gone)

			expectI := 1
			seen := 0
			for c, err := range m.All() {
				if err != nil {
					return err
				}
				kv, err := c.ReadKeyValuePair()
				if err != nil {
					return err
				}
				b, err := kv.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					return err
				}
				assertEqual(t, k(expectI), string(b))
				expectI += 2
				seen++
			}
			assertEqual(t, count/2, seen)

			// iterating-from on an unwritten (none) map yields nothing
			noneCursor := &ReadCursor{SlotPtr: SlotPointer{Position: nil, Slot: Slot{}}, DB: db}
			empty, err := NewReadSortedMap(noneCursor)
			if err != nil {
				return err
			}
			emptyCount := 0
			for range empty.AllFrom([]byte("anything")) {
				emptyCount++
			}
			for range empty.AllFromIndex(0) {
				emptyCount++
			}
			assertEqual(t, 0, emptyCount)

			// SortedSet with mixed short (inline) and long (external) keys
			setCursor, err := moment.PutCursor("set")
			if err != nil {
				return err
			}
			set, err := NewWriteSortedSet(setCursor)
			if err != nil {
				return err
			}
			if err := set.Put("short"); err != nil {
				return err
			}
			if err := set.Put("a-much-longer-key-stored-externally"); err != nil {
				return err
			}
			if err := set.Put("mid"); err != nil {
				return err
			}
			if err := set.Put("short"); err != nil { // dup is a no-op
				return err
			}
			if c, err := set.Count(); err != nil {
				return err
			} else {
				assertEqual(t, int64(3), c)
			}
			if ok, err := set.Contains("mid"); err != nil {
				return err
			} else {
				assertEqual(t, true, ok)
			}
			if ok, err := set.Contains("nope"); err != nil {
				return err
			} else {
				assertEqual(t, false, ok)
			}
			want := []string{"a-much-longer-key-stored-externally", "mid", "short"}
			sn := 0
			for c, err := range set.All() {
				if err != nil {
					return err
				}
				kv, err := c.ReadKeyValuePair()
				if err != nil {
					return err
				}
				b, err := kv.KeyCursor.ReadBytes(maxRead)
				if err != nil {
					return err
				}
				assertEqual(t, want[sn], string(b))
				sn++
			}
			assertEqual(t, 3, sn)
			if ok, err := set.Remove("mid"); err != nil {
				return err
			} else {
				assertEqual(t, true, ok)
			}
			if c, err := set.Count(); err != nil {
				return err
			} else {
				assertEqual(t, int64(2), c)
			}

			// immutability guards: positional access is read-only, and keys/entries
			// cannot be overwritten through the low-level path API
			if _, err := m.writeCursor.WritePath([]PathPart{SortedMapGetIndexPart{Index: 0}}); err != ErrWriteNotAllowed {
				t.Fatalf("expected ErrWriteNotAllowed, got %v", err)
			}
			if _, err := m.writeCursor.WritePath([]PathPart{
				SortedMapGetPart{Target: SortedMapGetKey{Key: []byte("k0001")}},
				WriteData{Data: NewBytes([]byte("x"))},
			}); err != ErrCursorNotWriteable {
				t.Fatalf("expected ErrCursorNotWriteable, got %v", err)
			}

			// a write that fails after the key is inserted (a missing key written
			// through the non-writeable key slot) must still leave the count
			// consistent with the tree, not inserted-but-uncounted
			countBeforeFailedWrite, err := m.Count()
			if err != nil {
				return err
			}
			if _, err := m.writeCursor.WritePath([]PathPart{
				SortedMapGetPart{Target: SortedMapGetKey{Key: []byte("missing-key")}},
				WriteData{Data: NewBytes([]byte("x"))},
			}); err != ErrCursorNotWriteable {
				t.Fatalf("expected ErrCursorNotWriteable, got %v", err)
			}
			if c, err := m.Count(); err != nil {
				return err
			} else {
				assertEqual(t, countBeforeFailedWrite+1, c)
			}
			if kv, err := m.GetKeyValuePair("missing-key"); err != nil {
				return err
			} else if kv == nil {
				t.Fatal("expected missing-key to be present after failed write")
			}
			if _, err := m.Remove("missing-key"); err != nil { // restore the map for the assertions below
				return err
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// the map persists in the committed moment
	{
		history, err := NewReadArrayList(db.RootCursor().ReadCursor)
		if err != nil {
			t.Fatal(err)
		}
		momentCursor, err := history.GetCursor(-1)
		if err != nil {
			t.Fatal(err)
		}
		moment, err := NewReadHashMap(momentCursor)
		if err != nil {
			t.Fatal(err)
		}
		mapCursor, err := moment.GetCursor("map")
		if err != nil {
			t.Fatal(err)
		}
		m, err := NewReadSortedMap(mapCursor)
		if err != nil {
			t.Fatal(err)
		}
		if c, err := m.Count(); err != nil {
			t.Fatal(err)
		} else {
			assertEqual(t, int64(count/2), c)
		}
		kv, err := m.GetIndexKeyValuePair(0)
		if err != nil {
			t.Fatal(err)
		}
		b, err := kv.KeyCursor.ReadBytes(maxRead)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "k0001", string(b))
	}

	// a second moment that inherits and mutates the map must not disturb the first
	{
		history, err := NewWriteArrayList(db.RootCursor())
		if err != nil {
			t.Fatal(err)
		}
		lastSlot, err := history.GetSlot(-1)
		if err != nil {
			t.Fatal(err)
		}
		err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
			moment, err := NewWriteHashMap(cursor)
			if err != nil {
				return err
			}
			mapCursor, err := moment.PutCursor("map")
			if err != nil {
				return err
			}
			m, err := NewWriteSortedMap(mapCursor)
			if err != nil {
				return err
			}
			if ok, err := m.Remove("k0001"); err != nil {
				return err
			} else {
				assertEqual(t, true, ok)
			}
			return m.Put("k0001", NewUint(7)) // not in moment 0
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	{
		history, err := NewReadArrayList(db.RootCursor().ReadCursor)
		if err != nil {
			t.Fatal(err)
		}
		// moment 0 (original) is unchanged: k0001 still present with value 1
		m0Cursor, err := history.GetCursor(0)
		if err != nil {
			t.Fatal(err)
		}
		m0, err := NewReadHashMap(m0Cursor)
		if err != nil {
			t.Fatal(err)
		}
		map0Cursor, err := m0.GetCursor("map")
		if err != nil {
			t.Fatal(err)
		}
		map0, err := NewReadSortedMap(map0Cursor)
		if err != nil {
			t.Fatal(err)
		}
		if c, err := map0.Count(); err != nil {
			t.Fatal(err)
		} else {
			assertEqual(t, int64(count/2), c)
		}
		c0, err := map0.GetCursor("k0001")
		if err != nil {
			t.Fatal(err)
		}
		v0, err := c0.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(1), v0)

		// moment 1 reflects the mutation: k0001 re-added with value 7
		m1Cursor, err := history.GetCursor(1)
		if err != nil {
			t.Fatal(err)
		}
		m1, err := NewReadHashMap(m1Cursor)
		if err != nil {
			t.Fatal(err)
		}
		map1Cursor, err := m1.GetCursor("map")
		if err != nil {
			t.Fatal(err)
		}
		map1, err := NewReadSortedMap(map1Cursor)
		if err != nil {
			t.Fatal(err)
		}
		if c, err := map1.Count(); err != nil {
			t.Fatal(err)
		} else {
			assertEqual(t, int64(count/2), c)
		}
		c1, err := map1.GetCursor("k0001")
		if err != nil {
			t.Fatal(err)
		}
		v1, err := c1.ReadUint()
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, uint64(7), v1)
	}
}

func TestAllFrom(t *testing.T) {
	// CoreMemory
	{
		testAllFrom(t, NewCoreMemory(), sha1Hasher())
	}
	// CoreFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		core := NewCoreFile(f)
		defer core.Close()
		testAllFrom(t, core, sha1Hasher())
	}
	// CoreBufferedFile
	{
		f, err := os.CreateTemp("", "database")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		core := NewCoreBufferedFileWithSize(f, 1024)
		defer core.Close()
		testAllFrom(t, core, sha1Hasher())
	}
}

func testAllFrom(t *testing.T, core Core, hasher Hasher) {
	t.Helper()
	if err := core.SetLength(0); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(core, hasher)
	if err != nil {
		t.Fatal(err)
	}

	// enough items to force several tiers in both the array-list radix trie
	// (16^2 = 256 > 200) and the linked-array-list b-tree
	const count = 200

	history, err := NewWriteArrayList(db.RootCursor())
	if err != nil {
		t.Fatal(err)
	}
	lastSlot, err := history.GetSlot(-1)
	if err != nil {
		t.Fatal(err)
	}
	err = history.AppendContext(lastSlot, func(cursor *WriteCursor) error {
		moment, err := NewWriteHashMap(cursor)
		if err != nil {
			return err
		}
		listCursor, err := moment.PutCursor("list")
		if err != nil {
			return err
		}
		list, err := NewWriteArrayList(listCursor)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if err := list.Append(NewUint(uint64(i))); err != nil {
				return err
			}
		}
		linkedCursor, err := moment.PutCursor("linked")
		if err != nil {
			return err
		}
		linked, err := NewWriteLinkedArrayList(linkedCursor)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if err := linked.Append(NewUint(uint64(i))); err != nil {
				return err
			}
		}

		// the write-side structs expose AllFrom too, yielding read-only cursors
		checkWrite := func(seq iter.Seq2[*ReadCursor, error], wantFirst, wantN int) error {
			first, n := -1, 0
			for c, err := range seq {
				if err != nil {
					return err
				}
				v, err := c.ReadUint()
				if err != nil {
					return err
				}
				if first < 0 {
					first = int(v)
				}
				n++
			}
			assertEqual(t, wantFirst, first)
			assertEqual(t, wantN, n)
			return nil
		}
		if err := checkWrite(list.AllFrom(count-3), count-3, 3); err != nil {
			return err
		}
		if err := checkWrite(linked.AllFrom(-2), count-2, 2); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	momentCursor, err := history.GetCursor(-1)
	if err != nil {
		t.Fatal(err)
	}
	moment, err := NewReadHashMap(momentCursor)
	if err != nil {
		t.Fatal(err)
	}
	listCursor, err := moment.GetCursor("list")
	if err != nil {
		t.Fatal(err)
	}
	list, err := NewReadArrayList(listCursor)
	if err != nil {
		t.Fatal(err)
	}
	linkedCursor, err := moment.GetCursor("linked")
	if err != nil {
		t.Fatal(err)
	}
	linked, err := NewReadLinkedArrayList(linkedCursor)
	if err != nil {
		t.Fatal(err)
	}

	// walk an iter.Seq2 and assert it yields resolved, resolved+1, .., count-1
	checkFrom := func(seq iter.Seq2[*ReadCursor, error], resolved int64) {
		expected := resolved
		for c, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			v, err := c.ReadUint()
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, uint64(expected), v)
			expected++
		}
		assertEqual(t, int64(count), expected)
	}

	// iteratorFrom(k) yields exactly k, k+1, .., count-1, for both types.
	// negative indexes count from the end: -1 starts at the last element, -count
	// at the first.
	starts := []int64{0, 1, 15, 16, 17, 100, count - 2, count - 1, -1, -2, -16, -100, -count}
	for _, start := range starts {
		resolved := start
		if start < 0 {
			resolved = count + start
		}
		checkFrom(list.AllFrom(start), resolved)
		checkFrom(linked.AllFrom(start), resolved)
	}

	// a start out of range (past the end, or more negative than -count) yields nothing
	for _, start := range []int64{count, count + 1, count + 1000, -count - 1, -count - 1000} {
		n := 0
		for _, err := range list.AllFrom(start) {
			if err != nil {
				t.Fatal(err)
			}
			n++
		}
		for _, err := range linked.AllFrom(start) {
			if err != nil {
				t.Fatal(err)
			}
			n++
		}
		assertEqual(t, 0, n)
	}
}
