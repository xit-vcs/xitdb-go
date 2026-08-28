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
* Reads never block writes, and a database can be read from multiple threads/processes without locks.
* No query engine of any kind. You just write data structures (primarily an `ArrayList` and `HashMap`) that can be nested arbitrarily.
* No dependencies besides the Go standard library (requires Go 1.23+).

This database was originally made for the [xit version control system](https://github.com/xit-vcs/xit), but I bet it has a lot of potential for other projects. The combination of being immutable and having an API similar to in-memory data structures is pretty powerful. Consider using it [instead of SQLite](https://gist.github.com/xeubie/03a0724484e1111ef4c05d72a935c42c) for your Go projects: it's simpler, it's pure Go, and it creates no impedance mismatch with your program the way SQL databases do.

* [Example](#example)
* [Initializing a Database](#initializing-a-database)
* [Types](#types)
* [Cloning and Undoing](#cloning-and-undoing)
* [Sorting and Paginating](#sorting-and-paginating)
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

// init the db
core := xitdb.NewCoreBufferedFile(f)
defer core.Close()
hasher := xitdb.Hasher{Hash: sha1.New()}
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
lastSlot, err := history.GetSlot(-1)
if err != nil {
    log.Fatal(err)
}
err = history.AppendContext(lastSlot, func(cursor *xitdb.WriteCursor) error {
    moment, err := xitdb.NewWriteHashMap(cursor)
    if err != nil {
        return err
    }

    if err := moment.Put("foo", xitdb.NewString("foo")); err != nil {
        return err
    }
    if err := moment.Put("bar", xitdb.NewString("bar")); err != nil {
        return err
    }

    fruitsCursor, err := moment.PutCursor("fruits")
    if err != nil {
        return err
    }
    fruits, err := xitdb.NewWriteArrayList(fruitsCursor)
    if err != nil {
        return err
    }
    if err := fruits.Append(xitdb.NewString("apple")); err != nil {
        return err
    }
    if err := fruits.Append(xitdb.NewString("pear")); err != nil {
        return err
    }
    if err := fruits.Append(xitdb.NewString("grape")); err != nil {
        return err
    }

    peopleCursor, err := moment.PutCursor("people")
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
    if err := alice.Put("name", xitdb.NewString("Alice")); err != nil {
        return err
    }
    if err := alice.Put("age", xitdb.NewUint(25)); err != nil {
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
    if err := bob.Put("name", xitdb.NewString("Bob")); err != nil {
        return err
    }
    if err := bob.Put("age", xitdb.NewUint(42)); err != nil {
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
fooCursor, err := moment.GetCursor("foo")
if err != nil {
    log.Fatal(err)
}
fooValue, err := fooCursor.ReadBytes(1024)
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(fooValue)) // "foo"

// to get the "fruits" list, we get the cursor to it and
// then pass it to the ArrayList constructor
fruitsCursor, err := moment.GetCursor("fruits")
if err != nil {
    log.Fatal(err)
}
fruits, err := xitdb.NewReadArrayList(fruitsCursor)
if err != nil {
    log.Fatal(err)
}
fruitsCount, err := fruits.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(fruitsCount) // 3

// now we can get the first item from the fruits list and read it
appleCursor, err := fruits.GetCursor(0)
if err != nil {
    log.Fatal(err)
}
appleValue, err := appleCursor.ReadBytes(maxRead)
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

Every `Core` implements `io.Closer` and owns the file passed to its constructor. Call `Close` on the core rather than on the underlying file so buffered data is flushed before the file is closed.

Usually, you want to use a top-level `ArrayList` like in the example above, because that allows you to store a reference to each copy of the database (which I call a "moment"). This is how it supports transactions, despite not having any rollback journal or write-ahead log. It's an append-only database, so the data you are writing is invisible to any reader until the very last step, when the top-level list's header is updated.

You can also use a top-level `HashMap`, which is useful for ephemeral databases where immutability or transaction safety isn't necessary. Since xitdb supports in-memory databases, you could use it as an over-the-wire serialization format. Much like "Cap'n Proto", xitdb has no encoding/decoding step: you just give the buffer to xitdb and it can immediately read from it.

## Types

In xitdb there are a variety of immutable data structures that you can nest arbitrarily:

* `HashMap` contains key-value pairs stored with a hash
* `HashSet` is like a `HashMap` that only sets the keys; it is useful when only checking for membership
* `CountedHashMap` and `CountedHashSet` are just a `HashMap` and `HashSet` that maintain a count of their contents
* `ArrayList` is a growable array
* `LinkedArrayList` is like an `ArrayList` that can also be efficiently sliced and concatenated
* `SortedMap` and `SortedSet` are like a `HashMap` and `HashSet` where the keys are byte arrays kept in lexicographic order

The `Hash`-based data structures and the `Arraylist` use the hash array mapped trie, invented by Phil Bagwell (originally made immutable and widely available by Rich Hickey in Clojure). The `LinkedArrayList`, `SortedMap`, and `SortedSet` are based on a B-tree.

There are also scalar types you can store in the above-mentioned data structures:

* `Bytes` is a byte array
* `Uint` is an unsigned 64-bit int
* `Int` is a signed 64-bit int
* `Float` is a 64-bit float

You may also want to define custom types. For example, you may want to store a big integer that can't fit in 64 bits. You could just store this with `Bytes`, but when reading the byte array there wouldn't be any indication that it should be interpreted as a big integer.

In xitdb, you can optionally store a format tag with a byte array. A format tag is a 2 byte tag that is stored alongside the byte array. Readers can use it to decide how to interpret the byte array. Here's an example of storing a random 256-bit number with `bi` as the format tag:

```go
randomBigInt := make([]byte, 32)
rand.Read(randomBigInt)
if err := moment.Put("random-number", xitdb.NewTaggedBytes(randomBigInt, []byte("bi"))); err != nil {
    return err
}
```

Then, you can read it like this:

```go
randomNumberCursor, err := moment.GetCursor("random-number")
if err != nil {
    log.Fatal(err)
}
randomNumber, err := randomNumberCursor.ReadBytesObject(1024)
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(randomNumber.FormatTag)) // "bi"
bigInt := new(big.Int).SetBytes(randomNumber.Value)
```

There are many types you may want to store this way. Maybe an ISO-8601 date like `2026-01-01T18:55:48Z` could be stored with `dt` as the format tag. It's also great for storing custom structs. Just define the struct, serialize it as a byte array using whatever mechanism you wish, and store it with a format tag. Keep in mind that format tags can be *any* 2 bytes, so there are 65536 possible format tags.

## Cloning and Undoing

A powerful feature of immutable data is fast cloning. Any data structure can be instantly cloned and changed without affecting the original. Starting with the example code above, we can make a new transaction that creates a "food" list based on the existing "fruits" list:

```go
lastSlot, err := history.GetSlot(-1)
if err != nil {
    log.Fatal(err)
}

err = history.AppendContext(lastSlot, func(cursor *xitdb.WriteCursor) error {
    moment, err := xitdb.NewWriteHashMap(cursor)
    if err != nil {
        return err
    }

    fruitsCursor, err := moment.GetCursor("fruits")
    if err != nil {
        return err
    }
    fruits, err := xitdb.NewReadArrayList(fruitsCursor)
    if err != nil {
        return err
    }

    // create a new key called "food" whose initial value is
    // based on the "fruits" list
    foodCursor, err := moment.PutCursor("food")
    if err != nil {
        return err
    }
    foodCursor.Write(fruits.Slot())

    food, err := xitdb.NewWriteArrayList(foodCursor)
    if err != nil {
        return err
    }
    if err := food.Append(xitdb.NewString("eggs")); err != nil {
        return err
    }
    if err := food.Append(xitdb.NewString("rice")); err != nil {
        return err
    }
    if err := food.Append(xitdb.NewString("fish")); err != nil {
        return err
    }

    return nil
})
if err != nil {
    log.Fatal(err)
}

momentCursor, err := history.GetCursor(-1)
if err != nil {
    log.Fatal(err)
}
moment, err := xitdb.NewReadHashMap(momentCursor)
if err != nil {
    log.Fatal(err)
}

// the food list includes the fruits
foodCursor, err := moment.GetCursor("food")
if err != nil {
    log.Fatal(err)
}
food, err := xitdb.NewReadArrayList(foodCursor)
if err != nil {
    log.Fatal(err)
}
foodCount, err := food.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(foodCount) // 6

// ...but the fruits list hasn't been changed
fruitsCursor, err := moment.GetCursor("fruits")
if err != nil {
    log.Fatal(err)
}
fruits, err := xitdb.NewReadArrayList(fruitsCursor)
if err != nil {
    log.Fatal(err)
}
fruitsCount, err := fruits.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(fruitsCount) // 3
```

Before we continue, let's save the latest history index, so we can revert back to this moment of the database later:

```go
historyCount, err := history.Count()
if err != nil {
    log.Fatal(err)
}
historyIndex := historyCount - 1
```

There's one catch you'll run into when cloning. If we try cloning a data structure that was created in the same transaction, it doesn't seem to work:

```go
lastSlot, err := history.GetSlot(-1)
if err != nil {
    log.Fatal(err)
}

err = history.AppendContext(lastSlot, func(cursor *xitdb.WriteCursor) error {
    moment, err := xitdb.NewWriteHashMap(cursor)
    if err != nil {
        return err
    }

    bigCitiesCursor, err := moment.PutCursor("big-cities")
    if err != nil {
        return err
    }
    bigCities, err := xitdb.NewWriteArrayList(bigCitiesCursor)
    if err != nil {
        return err
    }
    if err := bigCities.Append(xitdb.NewString("New York, NY")); err != nil {
        return err
    }
    if err := bigCities.Append(xitdb.NewString("Los Angeles, CA")); err != nil {
        return err
    }

    // create a new key called "cities" whose initial value is
    // based on the "big-cities" list
    citiesCursor, err := moment.PutCursor("cities")
    if err != nil {
        return err
    }
    citiesCursor.Write(bigCities.Slot())

    cities, err := xitdb.NewWriteArrayList(citiesCursor)
    if err != nil {
        return err
    }
    if err := cities.Append(xitdb.NewString("Charleston, SC")); err != nil {
        return err
    }
    if err := cities.Append(xitdb.NewString("Louisville, KY")); err != nil {
        return err
    }

    return nil
})
if err != nil {
    log.Fatal(err)
}

momentCursor, err := history.GetCursor(-1)
if err != nil {
    log.Fatal(err)
}
moment, err := xitdb.NewReadHashMap(momentCursor)
if err != nil {
    log.Fatal(err)
}

// the cities list contains all four
citiesCursor, err := moment.GetCursor("cities")
if err != nil {
    log.Fatal(err)
}
cities, err := xitdb.NewReadArrayList(citiesCursor)
if err != nil {
    log.Fatal(err)
}
citiesCount, err := cities.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(citiesCount) // 4

// ..but so does big-cities! we did not intend to mutate this
bigCitiesCursor, err := moment.GetCursor("big-cities")
if err != nil {
    log.Fatal(err)
}
bigCities, err := xitdb.NewReadArrayList(bigCitiesCursor)
if err != nil {
    log.Fatal(err)
}
bigCitiesCount, err := bigCities.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(bigCitiesCount) // 4
```

The reason that `big-cities` was mutated is because all data in a given transaction is temporarily mutable. This is a very important optimization, but in this case, it's not what we want.

To show how to fix this, let's first undo the transaction we just made. Here we use the `historyIndex` we saved before to revert back to the older database moment:

```go
historySlot, err := history.GetSlot(historyIndex)
if err != nil {
    log.Fatal(err)
}
if err := history.Append(historySlot); err != nil {
    log.Fatal(err)
}
```

This time, after making the "big cities" list, we call `Freeze`, which tells xitdb to consider all data made so far in the transaction to be immutable. After that, we can clone it into the "cities" list and it will work the way we wanted:

```go
lastSlot, err := history.GetSlot(-1)
if err != nil {
    log.Fatal(err)
}

err = history.AppendContext(lastSlot, func(cursor *xitdb.WriteCursor) error {
    moment, err := xitdb.NewWriteHashMap(cursor)
    if err != nil {
        return err
    }

    bigCitiesCursor, err := moment.PutCursor("big-cities")
    if err != nil {
        return err
    }
    bigCities, err := xitdb.NewWriteArrayList(bigCitiesCursor)
    if err != nil {
        return err
    }
    if err := bigCities.Append(xitdb.NewString("New York, NY")); err != nil {
        return err
    }
    if err := bigCities.Append(xitdb.NewString("Los Angeles, CA")); err != nil {
        return err
    }

    // freeze here, so big-cities won't be mutated
    if err := cursor.DB.Freeze(); err != nil {
        return err
    }

    // create a new key called "cities" whose initial value is
    // based on the "big-cities" list
    citiesCursor, err := moment.PutCursor("cities")
    if err != nil {
        return err
    }
    citiesCursor.Write(bigCities.Slot())

    cities, err := xitdb.NewWriteArrayList(citiesCursor)
    if err != nil {
        return err
    }
    if err := cities.Append(xitdb.NewString("Charleston, SC")); err != nil {
        return err
    }
    if err := cities.Append(xitdb.NewString("Louisville, KY")); err != nil {
        return err
    }

    return nil
})
if err != nil {
    log.Fatal(err)
}

momentCursor, err := history.GetCursor(-1)
if err != nil {
    log.Fatal(err)
}
moment, err := xitdb.NewReadHashMap(momentCursor)
if err != nil {
    log.Fatal(err)
}

// the cities list contains all four
citiesCursor, err := moment.GetCursor("cities")
if err != nil {
    log.Fatal(err)
}
cities, err := xitdb.NewReadArrayList(citiesCursor)
if err != nil {
    log.Fatal(err)
}
citiesCount, err := cities.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(citiesCount) // 4

// and big-cities only contains the original two
bigCitiesCursor, err := moment.GetCursor("big-cities")
if err != nil {
    log.Fatal(err)
}
bigCities, err := xitdb.NewReadArrayList(bigCitiesCursor)
if err != nil {
    log.Fatal(err)
}
bigCitiesCount, err := bigCities.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(bigCitiesCount) // 2
```

## Sorting and Paginating

The `Hash`-based structures are great for looking data up by key, but they store their contents in hash order, which is meaningless to a human. Real apps need to show data in a sensible order (such as users listed alphabetically) one page at a time. Relational databases like SQLite have this built-in: you declare a `CREATE INDEX`, write `ORDER BY username LIMIT 20 OFFSET 40`, and the query planner maintains the index for you.

In xitdb there are no built-in indexes, so you build and maintain them yourself. That's a little more code, but the index is just another data structure: a `SortedMap` whose keys sort the way you want. You keep it in sync by writing to it in the same transaction that writes the primary data.

Why a `SortedMap` and not an `ArrayList`? An `ArrayList` keeps things in insertion order, which is only useful when the order you want *is* the order you wrote them in. The moment you want a different order (alphabetical, by score, by anything that isn't "when it arrived") you need a structure that stays sorted by a key. A `SortedMap` does, and it can seek straight to the first key greater than or equal to a given value, which is what makes type-ahead search possible.

Let's model a user directory: a collection of users we look up by id, plus a secondary index that lists them alphabetically by username. The primary store is a `HashMap` from user id to the user's fields (like a row keyed by its primary key). The secondary index is a `SortedMap` keyed by username, whose value is the user id to look up.

A `SortedMap` orders its keys lexicographically by their raw bytes. For ASCII usernames that's just alphabetical order, and since usernames are unique, every key is already distinct, so the key is simply the username itself. For a sort key that *isn't* unique, like a score, you'd append the id to keep keys distinct. See the note at the end.

Now we write some users. Note they're inserted in arbitrary order; the index sorts them, so insertion order doesn't matter. On each insert we write the user into the primary map and add an entry to the secondary index (keeping both in sync is your job, not the database's):

```go
type User struct {
    ID       string
    Username string
    Name     string
}

// inserted in arbitrary order; the index will sort them alphabetically
newUsers := []User{
    {ID: "user000000000001", Username: "dave", Name: "Dave Smith"},
    {ID: "user000000000002", Username: "alice", Name: "Alice Jones"},
    {ID: "user000000000003", Username: "carol", Name: "Carol White"},
    {ID: "user000000000004", Username: "dan", Name: "Dan Brown"},
    {ID: "user000000000005", Username: "bob", Name: "Bob Lee"},
    {ID: "user000000000006", Username: "eve", Name: "Eve Adams"},
}

lastSlot, err := history.GetSlot(-1)
if err != nil {
    log.Fatal(err)
}
err = history.AppendContext(lastSlot, func(cursor *xitdb.WriteCursor) error {
    moment, err := xitdb.NewWriteHashMap(cursor)
    if err != nil {
        return err
    }

    // the primary store: a HashMap from user id to the user's fields
    idToUserCursor, err := moment.PutCursor("id->user")
    if err != nil {
        return err
    }
    idToUser, err := xitdb.NewWriteHashMap(idToUserCursor)
    if err != nil {
        return err
    }

    // the secondary index: a SortedMap ordered alphabetically by username.
    // there's no CREATE INDEX here, so we maintain it ourselves on every write.
    usernameToIDCursor, err := moment.PutCursor("username->id")
    if err != nil {
        return err
    }
    usernameToID, err := xitdb.NewWriteSortedMap(usernameToIDCursor)
    if err != nil {
        return err
    }

    for _, user := range newUsers {
        // write the user into the primary map under its id
        userCursor, err := idToUser.PutCursor(user.ID)
        if err != nil {
            return err
        }
        userMap, err := xitdb.NewWriteHashMap(userCursor)
        if err != nil {
            return err
        }
        if err := userMap.Put("username", xitdb.NewString(user.Username)); err != nil {
            return err
        }
        if err := userMap.Put("name", xitdb.NewString(user.Name)); err != nil {
            return err
        }

        // add an entry to the secondary index: the key is the username (the
        // sort key), and the value is the user id we'll use to look it back up.
        if err := usernameToID.Put(user.Username, xitdb.NewString(user.ID)); err != nil {
            return err
        }
    }
    return nil
})
if err != nil {
    log.Fatal(err)
}
```

To display a page, we walk the `SortedMap` instead of the `HashMap`. A web app would take a `pageSize` and an `after` offset from the request (something like `/users?after=20`), so this is the xitdb equivalent of `ORDER BY username LIMIT pageSize OFFSET after`:

```go
momentCursor, err := history.GetCursor(-1)
if err != nil {
    log.Fatal(err)
}
moment, err := xitdb.NewReadHashMap(momentCursor)
if err != nil {
    log.Fatal(err)
}

idToUserCursor, err := moment.GetCursor("id->user")
if err != nil {
    log.Fatal(err)
}
idToUser, err := xitdb.NewReadHashMap(idToUserCursor)
if err != nil {
    log.Fatal(err)
}

usernameToIDCursor, err := moment.GetCursor("username->id")
if err != nil {
    log.Fatal(err)
}
usernameToID, err := xitdb.NewReadSortedMap(usernameToIDCursor)
if err != nil {
    log.Fatal(err)
}

// a web request would supply these; here we just grab the first page
pageSize := int64(2)
after := int64(0)

count, err := usernameToID.Count()
if err != nil {
    log.Fatal(err)
}
end := after + pageSize
if end > count {
    end = count
}

// seek straight to the start of the page, then walk forward one entry at a
// time. because SortedMap is a count-augmented B+tree, AllFromIndex
// finds rank `after` in O(log n) without scanning the entries it skips, so
// jumping to page 500 is just as cheap as page 1.
i := after
for idCursor, err := range usernameToID.AllFromIndex(after) {
    if err != nil {
        log.Fatal(err)
    }
    if i >= end {
        break
    }

    idKv, err := idCursor.ReadKeyValuePair()
    if err != nil {
        log.Fatal(err)
    }

    // the index entry's value is the user id; use it to read the
    // full user out of the primary map
    userIDBytes, err := idKv.ValueCursor.ReadBytes(1024)
    if err != nil {
        log.Fatal(err)
    }

    userCursor, err := idToUser.GetCursor(string(userIDBytes))
    if err != nil {
        log.Fatal(err)
    }
    userMap, err := xitdb.NewReadHashMap(userCursor)
    if err != nil {
        log.Fatal(err)
    }
    nameCursor, err := userMap.GetCursor("name")
    if err != nil {
        log.Fatal(err)
    }
    name, err := nameCursor.ReadBytes(1024)
    if err != nil {
        log.Fatal(err)
    }

    // a real app would render this into the page's HTML
    fmt.Println(string(name))

    i++
}
```

Pagination by index is only half of what the ordering buys us. Because the index is sorted by username, we can also seek straight to a *key* (the first username greater than or equal to a prefix) and walk forward only as far as the prefix matches. That's a type-ahead search (think @-mention autocomplete), and it's the thing an `ArrayList` can't do: with no sorted index, there's nothing to seek into. We use `AllFrom` (which takes a key) instead of `AllFromIndex` (which takes a rank):

```go
// the user typed "da" into an @-mention box; find everyone whose username
// starts with it. AllFrom seeks to the first key >= "da" in O(log n),
// then we walk forward until a username no longer starts with the prefix.
prefix := []byte("da")
for idCursor, err := range usernameToID.AllFrom(prefix) {
    if err != nil {
        log.Fatal(err)
    }

    idKv, err := idCursor.ReadKeyValuePair()
    if err != nil {
        log.Fatal(err)
    }

    // the key is the username; stop once we've walked past the prefix
    username, err := idKv.KeyCursor.ReadBytes(1024)
    if err != nil {
        log.Fatal(err)
    }
    if !bytes.HasPrefix(username, prefix) {
        break
    }

    // a real app would offer this as a suggestion (here: "dan", then "dave")
    fmt.Println(string(username))
}
```

This works for any ordering you need: sort by a username with a string key like we did here, by score with a big-endian integer key (encode numbers big-endian so their byte order matches numeric order), or build several `SortedMap` indexes over the same primary `HashMap` to offer the data in different orders. When a sort key isn't unique (many users could share a score), append the id to keep every key distinct:

```go
// build a SortedMap key that sorts by score. the big-endian score makes byte
// order match numeric order; the user id is appended so two users with the
// same score still get distinct keys.
func orderKey(score uint64, userID []byte) []byte {
    key := make([]byte, 8+len(userID))
    binary.BigEndian.PutUint64(key[:8], score)
    copy(key[8:], userID)
    return key
}
```

With xitdb you "bring your own index". It takes a bit more effort than the declarative convenience of SQL databases, but it gives you more explicit control, and avoids the common problem in SQL where queries silently become inefficient due to not using indexes. In xitdb, inefficiency is hard to miss because you are always writing your queries as imperative code and the indexes are always explicit.

## Large Byte Arrays

When reading and writing large byte arrays, you probably don't want to have all of their contents in memory at once. To incrementally write to a byte array, just get a writer from a cursor:

```go
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
    bw.Write([]byte("hello, world\n"))
}
bw.Flush()
if err := cursorWriter.Finish(); err != nil {
    return err
}
```

If you need to set a format tag for the byte array, set the `FormatTag` field on the writer before you call `Finish`.

To read a byte array incrementally, get a reader from a cursor:

```go
longTextCursor, err := moment.GetCursor("long-text")
if err != nil {
    log.Fatal(err)
}
cursorReader, err := longTextCursor.Reader()
if err != nil {
    log.Fatal(err)
}
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
peopleCursor, err := moment.GetCursor("people")
if err != nil {
    log.Fatal(err)
}
people, err := xitdb.NewReadArrayList(peopleCursor)
if err != nil {
    log.Fatal(err)
}

for personCursor, err := range people.All() {
    if err != nil {
        log.Fatal(err)
    }
    person, err := xitdb.NewReadHashMap(personCursor)
    if err != nil {
        log.Fatal(err)
    }

    for kvPairCursor, err := range person.All() {
        if err != nil {
            log.Fatal(err)
        }
        kvPair, err := kvPairCursor.ReadKeyValuePair()
        if err != nil {
            log.Fatal(err)
        }

        key, err := kvPair.KeyCursor.ReadBytes(1024)
        if err != nil {
            log.Fatal(err)
        }

        switch kvPair.ValueCursor.SlotPtr.Slot.Tag {
        case xitdb.TagShortBytes, xitdb.TagBytes:
            val, err := kvPair.ValueCursor.ReadBytes(1024)
            if err != nil {
                log.Fatal(err)
            }
            fmt.Printf("%s: %s\n", key, val)
        case xitdb.TagUint:
            val, err := kvPair.ValueCursor.ReadUint()
            if err != nil {
                log.Fatal(err)
            }
            fmt.Printf("%s: %d\n", key, val)
        case xitdb.TagInt:
            val, err := kvPair.ValueCursor.ReadInt()
            if err != nil {
                log.Fatal(err)
            }
            fmt.Printf("%s: %d\n", key, val)
        case xitdb.TagFloat:
            val, err := kvPair.ValueCursor.ReadFloat()
            if err != nil {
                log.Fatal(err)
            }
            fmt.Printf("%s: %f\n", key, val)
        }
    }
}
```

The above code iterates over `people`, which is an `ArrayList`, and for each person (which is a `HashMap`), it iterates over each of its key-value pairs.

The iteration of the `HashMap` looks the same with `HashSet`, `CountedHashMap`, and `CountedHashSet`. When iterating, you call `ReadKeyValuePair` on the cursor and can read the `KeyCursor` and `ValueCursor` from it. In maps, `Put` sets the key and value. In sets, `Put` only sets the key; the value will always have a tag type of `TagNone`.

`ArrayList` and `LinkedArrayList` also have an `AllFrom` method, which starts the iterator from the given index. `SortedMap` and `SortedSet` have `AllFrom` and `AllFromIndex` to start the iterator from a key or index respectively. This is especially useful for pagination: you can seek straight to the start of a page and walk forward only as far as you need. See the [Sorting and Paginating](#sorting-and-paginating) section for an example.

## Hashing

The hashing data structures will create the hash for you when you call methods like `Put` or `GetCursor`. If you want to do the hashing yourself, there are methods like `PutByHash` and `GetCursorByHash` that take a `[]byte` as the hash.

When initializing a database, you tell xitdb how to hash with the `Hasher`. If you're using SHA-1, it will look like this:

```go
f, err := os.OpenFile("main.db", os.O_RDWR|os.O_CREATE, 0644)
if err != nil {
    log.Fatal(err)
}

core := xitdb.NewCoreFile(f)
defer core.Close()
hasher := xitdb.Hasher{Hash: sha1.New()}
db, err := xitdb.NewDatabase(core, hasher)
if err != nil {
    log.Fatal(err)
}
```

The size of the hash in bytes will be stored in the database's header. If you try opening it later with a hashing algorithm that has the wrong hash size, it will return an error. If you are unsure what hash size the database uses, this creates a chicken-and-egg problem. You can read the header before initializing the database like this:

```go
if err := core.SeekTo(0); err != nil {
    log.Fatal(err)
}
header, err := xitdb.ReadHeader(core)
if err != nil {
    log.Fatal(err)
}
fmt.Println(header.HashSize) // 20
```

The hash size alone does not disambiguate hashing algorithms, though. In addition, xitdb reserves four bytes in the header that you can use to put the name of the algorithm. You must provide it in the `Hasher`:

```go
hasher := xitdb.Hasher{
    Hash: sha1.New(),
    ID:   xitdb.BytesToID([4]byte{'s', 'h', 'a', '1'}),
}
```

The hash id is only written to the database header when it is first initialized. When you open it later, the hash id in the `Hasher` is ignored. You can read the hash id of an existing database like this:

```go
if err := core.SeekTo(0); err != nil {
    log.Fatal(err)
}
header, err := xitdb.ReadHeader(core)
if err != nil {
    log.Fatal(err)
}
fmt.Println(xitdb.IDToBytes(header.HashID)) // [4]byte{'s', 'h', 'a', '1'}
```

If you want to use SHA-256, I recommend using `sha2` as the hash id. You can then distinguish between SHA-256 and SHA-512 using the hash size, like this:

```go
if err := core.SeekTo(0); err != nil {
    log.Fatal(err)
}
header, err := xitdb.ReadHeader(core)
if err != nil {
    log.Fatal(err)
}

var hasher xitdb.Hasher
switch xitdb.IDToBytes(header.HashID) {
case [4]byte{'s', 'h', 'a', '1'}:
    hasher = xitdb.Hasher{
        Hash: sha1.New(),
        ID:   header.HashID,
    }
case [4]byte{'s', 'h', 'a', '2'}:
    switch header.HashSize {
    case 32:
        hasher = xitdb.Hasher{
            Hash: sha256.New(),
            ID:   header.HashID,
        }
    case 64:
        hasher = xitdb.Hasher{
            Hash: sha512.New(),
            ID:   header.HashID,
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
compactFile, err := os.OpenFile("compact.db", os.O_RDWR|os.O_CREATE, 0644)
if err != nil {
    log.Fatal(err)
}

compactCore := xitdb.NewCoreBufferedFile(compactFile)
defer compactCore.Close()
compactDb, err := db.Compact(compactCore)
if err != nil {
    log.Fatal(err)
}

// read from the new compacted db
history, err := xitdb.NewReadArrayList(compactDb.RootCursor().ReadCursor)
if err != nil {
    log.Fatal(err)
}
historyCount, err := history.Count()
if err != nil {
    log.Fatal(err)
}
fmt.Println(historyCount) // 1
```

This compacted database will be in a separate file. If you want to delete the original database and replace it with this one, you'll need to do that yourself. It is not possible to compact a database in-place (using the same file as the target database); doing so would fail and would render your original database unreadable.

## Thread Safety

It is possible to read the database from multiple threads/goroutines without locks, even while writes are happening. This is a big benefit of immutable databases. However, each thread needs to use its own `Database` instance. See [the multithreading test](https://github.com/xit-vcs/xitdb-go/blob/22c320fd08cd482ebf9fcfcb45e4bafb19e2a84e/high_level_test.go#L555) for an example of this. Also, keep in mind that writes still need to come from one thread at a time.
