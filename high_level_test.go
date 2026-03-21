package xitdb

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"io"
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
		defer f.Close()

		core := NewCoreFile(f)
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
		defer f.Close()

		core := NewCoreBufferedFileWithSize(f, 1024)
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
	defer f.Close()

	core := NewCoreFile(f)
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
	defer f.Close()

	core := NewCoreFile(f)
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
		defer f2.Close()

		core2 := NewCoreFile(f2)
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

	// the db size remains the same after writing junk data
	// and then reinitializing the db. this is useful because
	// there could be data from a transaction that never
	// completed due to an unclean shutdown.
	{
		coreLen, err := core.Length()
		if err != nil {
			t.Fatal(err)
		}
		if err := core.SeekTo(coreLen); err != nil {
			t.Fatal(err)
		}
		sizeBefore := coreLen

		if err := core.Write([]byte("this is junk data that will be deleted during init")); err != nil {
			t.Fatal(err)
		}

		// no error is thrown if db file is opened in read-only mode
		if fileMaybe != nil {
			readOnlyFile, err := os.Open(fileMaybe.Name())
			if err != nil {
				t.Fatal(err)
			}
			defer readOnlyFile.Close()
			readOnlyCore := NewCoreFile(readOnlyFile)
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
		assertEqual(t, sizeBefore, sizeAfter)
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
		defer sf.Close()
		tf, err := os.CreateTemp("", "compact_target")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tf.Name())
		defer tf.Close()

		sourceCore := NewCoreFile(sf)
		targetCore := NewCoreFile(tf)
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
		defer sf.Close()
		tf, err := os.CreateTemp("", "compact_target")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tf.Name())
		defer tf.Close()

		sourceCore := NewCoreBufferedFile(sf)
		targetCore := NewCoreBufferedFile(tf)
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
				return moment.Put("key1", NewString("final_value"))
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
				return moment.Put("changing_key", NewUint(uint64(r + 100)))
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
