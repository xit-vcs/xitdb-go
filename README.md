<p align="center">
  xitdb is an immutable database written in Go
  <br/>
  <br/>
  <b>Choose your flavor:</b>
  <a href="https://github.com/xit-vcs/xitdb">Zig</a> |
  <a href="https://github.com/xit-vcs/xitdb-java">Java</a> |
  <a href="https://github.com/codeboost/xitdb-clj">Clojure</a> |
  <a href="https://github.com/xit-vcs/xitdb-ts">TypeScript</a> |
  <a href="https://github.com/xit-vcs/xitdb-go">Go</a>
</p>

* Each transaction efficiently creates a new "copy" of the database, and past copies can still be read from and reverted to.
* Supports storing in a single file as well as purely in-memory use.
* Runs as a library (embedded in process).
* Incrementally reads and writes, so file-based databases can contain larger-than-memory datasets.
* Reads never block writes, and a database can be read from multiple goroutines/processes without locks.
* No query engine of any kind. You just write data structures (primarily an `ArrayList` and `HashMap`) that can be nested arbitrarily.
* No dependencies besides the Go standard library (requires Go 1.23+).

This database was originally made for the [xit version control system](https://github.com/xit-vcs/xit), but I bet it has a lot of potential for other projects. The combination of being immutable and having an API similar to in-memory data structures is pretty powerful. Consider using it [instead of SQLite](https://gist.github.com/radarroark/03a0724484e1111ef4c05d72a935c42c) for your Go projects: it's simpler, it's pure Go, and it creates no impedance mismatch with your program the way SQL databases do.

* [Example](#example)
* [Initializing a Database](#initializing-a-database)
* [Types](#types)
* [Cloning and Undoing](#cloning-and-undoing)
* [Large Byte Arrays](#large-byte-arrays)
* [Iterators](#iterators)
* [Hashing](#hashing)
* [Compaction](#compaction)
* [Thread Safety](#thread-safety)

## Example

In this example, we create a new database, write some data in a transaction, and read the data afterwards.

```go
f, err := os.OpenFile("main.db", os.O_RDWR|os.O_CREATE, 0644)
if err != nil {
    log.Fatal(err)
}
defer f.Close()

// init the db
core := xitdb.NewCoreBufferedFile(f)
hasher := xitdb.Hasher{NewHash: func() hash.Hash { return sha1.New() }}
db, err := xitdb.NewDatabase(core, hasher)
if err != nil {
    log.Fatal(err)
}

// to get the benefits of immutability, the top-level data structure
// must be an ArrayList, so each transaction is stored as an item in it
history, err := xitdb.NewWriteArrayList(db.RootCursor())
if err != nil {
    log.Fatal(err)
}

// this is how a transaction is executed. we call history.AppendContext,
// providing it with the most recent copy of the db and a context
// function. the function will run before the transaction has completed.
// this is where we can write changes to the db. if any error happens
// in it, the transaction will not complete and the db will be unaffected.
//
// after this transaction, the db will look like this if represented
// as JSON (in reality the format is binary):
//
// {"foo": "foo",
//  "bar": "bar",
//  "fruits": ["apple", "pear", "grape"],
//  "people": [
//    {"name": "Alice", "age": 25},
//    {"name": "Bob", "age": 42}
//  ]}
lastSlot, err := history.GetSlotAt(-1)
if err != nil {
    log.Fatal(err)
}
var slotData xitdb.WriteableData
if lastSlot != nil {
    slotData = *lastSlot
}

err = history.AppendContext(slotData, func(cursor *xitdb.WriteCursor) error {
    moment, err := xitdb.NewWriteHashMap(cursor)
    if err != nil {
        return err
    }

    if err := moment.PutString("foo", xitdb.NewBytesDataFromString("foo")); err != nil {
        return err
    }
    if err := moment.PutString("bar", xitdb.NewBytesDataFromString("bar")); err != nil {
        return err
    }

    fruitsCursor, err := moment.PutCursorByString("fruits")
    if err != nil {
        return err
    }
    fruits, err := xitdb.NewWriteArrayList(fruitsCursor)
    if err != nil {
        return err
    }
    if err := fruits.Append(xitdb.NewBytesDataFromString("apple")); err != nil {
        return err
    }
    if err := fruits.Append(xitdb.NewBytesDataFromString("pear")); err != nil {
        return err
    }
    if err := fruits.Append(xitdb.NewBytesDataFromString("grape")); err != nil {
        return err
    }

    peopleCursor, err := moment.PutCursorByString("people")
    if err != nil {
        return err
    }
    people, err := xitdb.NewWriteArrayList(peopleCursor)
    if err != nil {
        return err
    }

    aliceCursor, err := people.AppendCursor()
    if err != nil {
        return err
    }
    alice, err := xitdb.NewWriteHashMap(aliceCursor)
    if err != nil {
        return err
    }
    if err := alice.PutString("name", xitdb.NewBytesDataFromString("Alice")); err != nil {
        return err
    }
    if err := alice.PutString("age", xitdb.UintData{Value: 25}); err != nil {
        return err
    }

    bobCursor, err := people.AppendCursor()
    if err != nil {
        return err
    }
    bob, err := xitdb.NewWriteHashMap(bobCursor)
    if err != nil {
        return err
    }
    if err := bob.PutString("name", xitdb.NewBytesDataFromString("Bob")); err != nil {
        return err
    }
    if err := bob.PutString("age", xitdb.UintData{Value: 42}); err != nil {
        return err
    }

    return nil
})
if err != nil {
    log.Fatal(err)
}

// get the most recent copy of the database, like a moment
// in time. the -1 index will return the last index in the list.
momentCursor, err := history.GetCursor(-1)
if err != nil {
    log.Fatal(err)
}
moment, err := xitdb.NewReadHashMap(momentCursor)
if err != nil {
    log.Fatal(err)
}

// we can read the value of "foo" from the map by getting
// the cursor to "foo" and then calling ReadBytes on it
fooCursor, err := moment.GetCursorByString("foo")
if err != nil {
    log.Fatal(err)
}
maxRead := int64(1024)
fooValue, err := fooCursor.ReadBytes(&maxRead)
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(fooValue)) // "foo"

// to get the "fruits" list, we get the cursor to it and
// then pass it to the ArrayList constructor
fruitsCursor, err := moment.GetCursorByString("fruits")
if err != nil {
    log.Fatal(err)
}
fruits, err := xitdb.NewReadArrayList(fruitsCursor)
if err != nil {
    log.Fatal(err)
}
fruitsCount, _ := fruits.Count()
fmt.Println(fruitsCount) // 3

// now we can get the first item from the fruits list and read it
appleCursor, err := fruits.GetCursor(0)
if err != nil {
    log.Fatal(err)
}
appleValue, err := appleCursor.ReadBytes(&maxRead)
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(appleValue)) // "apple"
```

## Initializing a Database

A `Database` is initialized with an implementation of the `Core` interface, which determines how the i/o is done. There are three implementations of `Core` in this library: `CoreBufferedFile`, `CoreFile`, and `CoreMemory`.

* `CoreBufferedFile` databases, like in the example above, write to a file while using an in-memory buffer to dramatically improve performance. This is highly recommended if you want to create a file-based database. Initialize with `NewCoreBufferedFile(f)` where `f` is an `*os.File`.
* `CoreFile` databases use no buffering when reading and writing data. Initialize with `NewCoreFile(f)`. This is almost never necessary but it's useful as a benchmark comparison with `CoreBufferedFile` databases.
* `CoreMemory` databases work completely in memory. Initialize with `NewCoreMemory()`.

Usually, you want to use a top-level `ArrayList` like in the example above, because that allows you to store a reference to each copy of the database (which I call a "moment"). This is how it supports transactions, despite not having any rollback journal or write-ahead log. It's an append-only database, so the data you are writing is invisible to any reader until the very last step, when the top-level list's header is updated.

You can also use a top-level `HashMap`, which is useful for ephemeral databases where immutability or transaction safety isn't necessary. Since xitdb supports in-memory databases, you could use it as an over-the-wire serialization format. Much like "Cap'n Proto", xitdb has no encoding/decoding step: you just give the buffer to xitdb and it can immediately read from it.

## Types

In xitdb there are a variety of immutable data structures that you can nest arbitrarily:

* `HashMap` contains key-value pairs stored with a hash
* `HashSet` is like a `HashMap` that only sets the keys; it is useful when only checking for membership
* `CountedHashMap` and `CountedHashSet` are just a `HashMap` and `HashSet` that maintain a count of their contents
* `ArrayList` is a growable array
* `LinkedArrayList` is like an `ArrayList` that can also be efficiently sliced and concatenated

All data structures use the hash array mapped trie, invented by Phil Bagwell. The `LinkedArrayList` is based on his later work on RRB trees. These data structures were originally made immutable and widely available by Rich Hickey in Clojure. To my knowledge, they haven't been available in any open source database until xitdb.

There are also scalar types you can store in the above-mentioned data structures:

* `BytesData` is a byte array
* `UintData` is an unsigned 64-bit int
* `IntData` is a signed 64-bit int
* `FloatData` is a 64-bit float

You may also want to define custom types. For example, you may want to store a big integer that can't fit in 64 bits. You could just store this with `BytesData`, but when reading the byte array there wouldn't be any indication that it should be interpreted as a big integer.

In xitdb, you can optionally store a format tag with a byte array. A format tag is a 2 byte tag that is stored alongside the byte array. Readers can use it to decide how to interpret the byte array. Here's an example of storing a random 256-bit number with `bi` as the format tag:

```go
randomBigInt := make([]byte, 32)
rand.Read(randomBigInt)
moment.PutString("random-number", xitdb.NewBytesDataWithFormat(randomBigInt, []byte("bi")))
```

Then, you can read it like this:

```go
randomNumberCursor, _ := moment.GetCursorByString("random-number")
maxRead := int64(1024)
randomNumber, _ := randomNumberCursor.ReadBytesObject(&maxRead)
fmt.Println(string(randomNumber.FormatTag)) // "bi"
bigInt := new(big.Int).SetBytes(randomNumber.Value)
```

There are many types you may want to store this way. Maybe an ISO-8601 date like `2026-01-01T18:55:48Z` could be stored with `dt` as the format tag. It's also great for storing custom structs. Just define the struct, serialize it as a byte array using whatever mechanism you wish, and store it with a format tag. Keep in mind that format tags can be *any* 2 bytes, so there are 65536 possible format tags.

## Cloning and Undoing

A powerful feature of immutable data is fast cloning. Any data structure can be instantly cloned and changed without affecting the original. Starting with the example code above, we can make a new transaction that creates a "food" list based on the existing "fruits" list:

```go
lastSlot, _ := history.GetSlotAt(-1)
var slotData xitdb.WriteableData
if lastSlot != nil {
    slotData = *lastSlot
}

history.AppendContext(slotData, func(cursor *xitdb.WriteCursor) error {
    moment, _ := xitdb.NewWriteHashMap(cursor)

    fruitsCursor, _ := moment.GetCursorByString("fruits")
    fruits, _ := xitdb.NewReadArrayList(fruitsCursor)

    // create a new key called "food" whose initial value is
    // based on the "fruits" list
    foodCursor, _ := moment.PutCursorByString("food")
    foodCursor.WriteValue(fruits.GetSlot())

    food, _ := xitdb.NewWriteArrayList(foodCursor)
    food.Append(xitdb.NewBytesDataFromString("eggs"))
    food.Append(xitdb.NewBytesDataFromString("rice"))
    food.Append(xitdb.NewBytesDataFromString("fish"))

    return nil
})

momentCursor, _ := history.GetCursor(-1)
moment, _ := xitdb.NewReadHashMap(momentCursor)

// the food list includes the fruits
foodCursor, _ := moment.GetCursorByString("food")
food, _ := xitdb.NewReadArrayList(foodCursor)
foodCount, _ := food.Count()
fmt.Println(foodCount) // 6

// ...but the fruits list hasn't been changed
fruitsCursor, _ := moment.GetCursorByString("fruits")
fruits, _ := xitdb.NewReadArrayList(fruitsCursor)
fruitsCount, _ := fruits.Count()
fmt.Println(fruitsCount) // 3
```

Before we continue, let's save the latest history index, so we can revert back to this moment of the database later:

```go
historyCount, _ := history.Count()
historyIndex := historyCount - 1
```

There's one catch you'll run into when cloning. If we try cloning a data structure that was created in the same transaction, it doesn't seem to work:

```go
lastSlot, _ := history.GetSlotAt(-1)
var slotData xitdb.WriteableData
if lastSlot != nil {
    slotData = *lastSlot
}

history.AppendContext(slotData, func(cursor *xitdb.WriteCursor) error {
    moment, _ := xitdb.NewWriteHashMap(cursor)

    bigCitiesCursor, _ := moment.PutCursorByString("big-cities")
    bigCities, _ := xitdb.NewWriteArrayList(bigCitiesCursor)
    bigCities.Append(xitdb.NewBytesDataFromString("New York, NY"))
    bigCities.Append(xitdb.NewBytesDataFromString("Los Angeles, CA"))

    // create a new key called "cities" whose initial value is
    // based on the "big-cities" list
    citiesCursor, _ := moment.PutCursorByString("cities")
    citiesCursor.WriteValue(bigCities.GetSlot())

    cities, _ := xitdb.NewWriteArrayList(citiesCursor)
    cities.Append(xitdb.NewBytesDataFromString("Charleston, SC"))
    cities.Append(xitdb.NewBytesDataFromString("Louisville, KY"))

    return nil
})

momentCursor, _ := history.GetCursor(-1)
moment, _ := xitdb.NewReadHashMap(momentCursor)

// the cities list contains all four
citiesCursor, _ := moment.GetCursorByString("cities")
cities, _ := xitdb.NewReadArrayList(citiesCursor)
citiesCount, _ := cities.Count()
fmt.Println(citiesCount) // 4

// ..but so does big-cities! we did not intend to mutate this
bigCitiesCursor, _ := moment.GetCursorByString("big-cities")
bigCities, _ := xitdb.NewReadArrayList(bigCitiesCursor)
bigCitiesCount, _ := bigCities.Count()
fmt.Println(bigCitiesCount) // 4
```

The reason that `big-cities` was mutated is because all data in a given transaction is temporarily mutable. This is a very important optimization, but in this case, it's not what we want.

To show how to fix this, let's first undo the transaction we just made. Here we use the `historyIndex` we saved before to revert back to the older database moment:

```go
historySlot, _ := history.GetSlotAt(historyIndex)
history.Append(*historySlot)
```

This time, after making the "big cities" list, we call `Freeze`, which tells xitdb to consider all data made so far in the transaction to be immutable. After that, we can clone it into the "cities" list and it will work the way we wanted:

```go
lastSlot, _ := history.GetSlotAt(-1)
var slotData xitdb.WriteableData
if lastSlot != nil {
    slotData = *lastSlot
}

history.AppendContext(slotData, func(cursor *xitdb.WriteCursor) error {
    moment, _ := xitdb.NewWriteHashMap(cursor)

    bigCitiesCursor, _ := moment.PutCursorByString("big-cities")
    bigCities, _ := xitdb.NewWriteArrayList(bigCitiesCursor)
    bigCities.Append(xitdb.NewBytesDataFromString("New York, NY"))
    bigCities.Append(xitdb.NewBytesDataFromString("Los Angeles, CA"))

    // freeze here, so big-cities won't be mutated
    cursor.DB.Freeze()

    // create a new key called "cities" whose initial value is
    // based on the "big-cities" list
    citiesCursor, _ := moment.PutCursorByString("cities")
    citiesCursor.WriteValue(bigCities.GetSlot())

    cities, _ := xitdb.NewWriteArrayList(citiesCursor)
    cities.Append(xitdb.NewBytesDataFromString("Charleston, SC"))
    cities.Append(xitdb.NewBytesDataFromString("Louisville, KY"))

    return nil
})

momentCursor, _ := history.GetCursor(-1)
moment, _ := xitdb.NewReadHashMap(momentCursor)

// the cities list contains all four
citiesCursor, _ := moment.GetCursorByString("cities")
cities, _ := xitdb.NewReadArrayList(citiesCursor)
citiesCount, _ := cities.Count()
fmt.Println(citiesCount) // 4

// and big-cities only contains the original two
bigCitiesCursor, _ := moment.GetCursorByString("big-cities")
bigCities, _ := xitdb.NewReadArrayList(bigCitiesCursor)
bigCitiesCount, _ := bigCities.Count()
fmt.Println(bigCitiesCount) // 2
```

## Large Byte Arrays

When reading and writing large byte arrays, you probably don't want to have all of their contents in memory at once. To incrementally write to a byte array, just get a writer from a cursor:

```go
longTextCursor, _ := moment.PutCursorByString("long-text")
cursorWriter, _ := longTextCursor.Writer()
bw := bufio.NewWriter(cursorWriter)
for i := 0; i < 50; i++ {
    bw.Write([]byte("hello, world\n"))
}
bw.Flush()
cursorWriter.Finish() // remember to call this!
```

If you need to set a format tag for the byte array, set the `formatTag` field on the writer before you call `Finish`.

To read a byte array incrementally, get a reader from a cursor:

```go
longTextCursor, _ := moment.GetCursorByString("long-text")
cursorReader, _ := longTextCursor.Reader()
scanner := bufio.NewScanner(cursorReader)
count := 0
for scanner.Scan() {
    count++
}
fmt.Println(count) // 50
```

## Iterators

All data structures support iteration using Go 1.23's range-over-func iterators. Here's an example of iterating over an `ArrayList` and printing all of the keys and values of each `HashMap` contained in it:

```go
peopleCursor, _ := moment.GetCursorByString("people")
people, _ := xitdb.NewReadArrayList(peopleCursor)

for personCursor, err := range people.All() {
    if err != nil {
        log.Fatal(err)
    }
    person, _ := xitdb.NewReadHashMap(personCursor)

    for kvPairCursor, err := range person.All() {
        if err != nil {
            log.Fatal(err)
        }
        kvPair, _ := kvPairCursor.ReadKeyValuePair()

        maxRead := int64(1024)
        key, _ := kvPair.KeyCursor.ReadBytes(&maxRead)

        switch kvPair.ValueCursor.SlotPtr.Slot.Tag {
        case xitdb.TagShortBytes, xitdb.TagBytes:
            val, _ := kvPair.ValueCursor.ReadBytes(&maxRead)
            fmt.Printf("%s: %s\n", key, val)
        case xitdb.TagUint:
            val, _ := kvPair.ValueCursor.ReadUint()
            fmt.Printf("%s: %d\n", key, val)
        case xitdb.TagInt:
            val, _ := kvPair.ValueCursor.ReadInt()
            fmt.Printf("%s: %d\n", key, val)
        case xitdb.TagFloat:
            val, _ := kvPair.ValueCursor.ReadFloat()
            fmt.Printf("%s: %f\n", key, val)
        }
    }
}
```

The above code iterates over `people`, which is an `ArrayList`, and for each person (which is a `HashMap`), it iterates over each of its key-value pairs.

The iteration of the `HashMap` looks the same with `HashSet`, `CountedHashMap`, and `CountedHashSet`. When iterating, you call `ReadKeyValuePair` on the cursor and can read the `KeyCursor` and `ValueCursor` from it. In maps, `PutString` sets the key and value. In sets, `PutString` only sets the key; the value will always have a tag type of `TagNone`.

## Hashing

The hashing data structures will create the hash for you when you call methods like `PutString` or `GetCursorByString` and provide the key as a `string`. If you want to do the hashing yourself, there are methods like `PutByHash` and `GetCursorByHash` that take a `[]byte` as the hash.

When initializing a database, you tell xitdb how to hash with the `Hasher`. If you're using SHA-1, it will look like this:

```go
f, _ := os.OpenFile("main.db", os.O_RDWR|os.O_CREATE, 0644)
defer f.Close()

core := xitdb.NewCoreFile(f)
hasher := xitdb.Hasher{NewHash: func() hash.Hash { return sha1.New() }}
db, _ := xitdb.NewDatabase(core, hasher)
```

The size of the hash in bytes will be stored in the database's header. If you try opening it later with a hashing algorithm that has the wrong hash size, it will return an error. If you are unsure what hash size the database uses, this creates a chicken-and-egg problem. You can read the header before initializing the database like this:

```go
core.SeekTo(0)
header, _ := xitdb.ReadHeader(core)
fmt.Println(header.HashSize) // 20
```

The hash size alone does not disambiguate hashing algorithms, though. In addition, xitdb reserves four bytes in the header that you can use to put the name of the algorithm. You must provide it in the `Hasher`:

```go
id, _ := xitdb.StringToID("sha1")
hasher := xitdb.Hasher{
    NewHash: func() hash.Hash { return sha1.New() },
    ID:      id,
}
```

The hash id is only written to the database header when it is first initialized. When you open it later, the hash id in the `Hasher` is ignored. You can read the hash id of an existing database like this:

```go
core.SeekTo(0)
header, _ := xitdb.ReadHeader(core)
fmt.Println(xitdb.IDToString(header.HashID)) // "sha1"
```

If you want to use SHA-256, I recommend using `sha2` as the hash id. You can then distinguish between SHA-256 and SHA-512 using the hash size, like this:

```go
core.SeekTo(0)
header, _ := xitdb.ReadHeader(core)

var hasher xitdb.Hasher
switch xitdb.IDToString(header.HashID) {
case "sha1":
    hasher = xitdb.Hasher{
        NewHash: func() hash.Hash { return sha1.New() },
        ID:      header.HashID,
    }
case "sha2":
    switch header.HashSize {
    case 32:
        hasher = xitdb.Hasher{
            NewHash: func() hash.Hash { return sha256.New() },
            ID:      header.HashID,
        }
    case 64:
        hasher = xitdb.Hasher{
            NewHash: func() hash.Hash { return sha512.New() },
            ID:      header.HashID,
        }
    default:
        log.Fatal("Invalid hash size")
    }
default:
    log.Fatal("Invalid hash algorithm")
}
```

## Compaction

Normally, an immutable database grows forever, because old data is never deleted. To reclaim disk space and clear the history, xitdb supports compaction. This involves completely rebuilding the database file to only contain the data accessible from the latest copy (i.e., "moment") of the database.

```go
compactFile, _ := os.OpenFile("compact.db", os.O_RDWR|os.O_CREATE, 0644)
defer compactFile.Close()

compactCore := xitdb.NewCoreBufferedFile(compactFile)
compactDb, _ := db.Compact(compactCore, hasher)

// read from the new compacted db
history, _ := xitdb.NewReadArrayList(compactDb.RootCursor().ReadCursor)
historyCount, _ := history.Count()
fmt.Println(historyCount) // 1
```

This compacted database will be in a separate file. If you want to delete the original database and replace it with this one, you'll need to do that yourself. It is not possible to compact a database in-place (using the same file as the target database); doing so would fail and would render your original database unreadable.

## Thread Safety

It is possible to read the database from multiple threads/goroutines without locks, even while writes are happening. This is a big benefit of immutable databases. However, each thread needs to use its own `Database` instance. See [the multithreading test](https://github.com/xit-vcs/xitdb-go/blob/e898ba337848c0e46fe987f73a3f67aa9f8bd0d9/high_level_test.go#L568) for an example of this. Also, keep in mind that writes still need to come from one thread at a time.
