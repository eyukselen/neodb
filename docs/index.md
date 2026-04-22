# NeoDB

A simple, in-memory key-value store with indexing capability.

## Features

- Store key-value pairs with any value type
- Optional indexing on records for fast lookups
- Find keys by index name and value
- Organize data into named collections

## Installation

```bash
pip install neodb
```

## Quick Start

```python
from neodb.core import NeoDB

# Create a database
db = NeoDB("mydb")

# Create a collection
users = db.collection("users")

# Store records with optional indexes
users.put("user-1", {"name": "Alice", "age": 30}, indexes={"role": "admin"})
users.put("user-2", {"name": "Bob", "age": 25}, indexes={"role": "member"})
users.put("user-3", {"name": "Charlie", "age": 35}, indexes={"role": "admin"})

# Retrieve a record by key
user = users.get("user-1")
# {"name": "Alice", "age": 30}

# Find keys by index
admins = users.find_keys("role", "admin")
# {"user-1", "user-3"}

# Delete a record (indexes are cleaned up automatically)
users.delete("user-2")
```

## API Reference

### NeoDB

The top-level database object that manages collections.

#### `NeoDB(dbname="neodb")`

Create a new database instance.

```python
db = NeoDB()           # default name "neodb"
db = NeoDB("mydb")     # custom name
```

#### `db.collection(collection_name=None)`

Get or create a collection. If no name is provided, an auto-generated name is used (`Collection_1`, `Collection_2`, etc.). Calling with the same name returns the existing collection.

```python
users = db.collection("users")     # named collection
temp = db.collection()             # auto-named: "Collection_1"
```

#### `db.list_collections()`

Returns a list of collection names.

```python
db.list_collections()
# ["users"]
```

#### `db.drop_collection(collection_name)`

Drop a collection by name. Returns `True` if the collection existed, `False` otherwise.

```python
db.drop_collection("users")   # True
db.drop_collection("nope")    # False
```

---

### NeoCollection

A key-value store with optional indexing support.

#### `collection.put(key, value, indexes=None)`

Store a value under the given key. Optionally provide indexes as a dictionary of `{index_name: index_value}` pairs. If the key already exists, the old value and its indexes are replaced.

```python
users.put("user-1", {"name": "Alice"}, indexes={"role": "admin", "dept": "engineering"})
```

#### `collection.get(key)`

Retrieve the value for a key. Returns `None` if the key does not exist.

```python
users.get("user-1")    # {"name": "Alice"}
users.get("missing")   # None
```

#### `collection.delete(key)`

Delete a record and clean up its indexes. Returns `True` if the key existed, `False` otherwise.

```python
users.delete("user-1")    # True
users.delete("missing")   # False
```

#### `collection.find_keys(index_name, index_value)`

Find all keys that match a given index name and value. Returns a `set` of keys, or an empty set if no match is found.

```python
users.find_keys("role", "admin")    # {"user-1", "user-3"}
users.find_keys("role", "guest")    # set()
```

## How Indexing Works

When you store a record with indexes, NeoDB maintains two internal structures:

- **`indexes`** — a forward index mapping `index_name → index_value → {keys}` for fast lookups
- **`reverse_indexes`** — a reverse mapping `key → {index_name: index_value}` for efficient cleanup on delete or update

This means:

- **Lookups** via `find_keys()` are O(1) dictionary lookups
- **Deletes** and **updates** automatically clean up stale index entries
- **No manual index management** is required

```python
col = db.collection("products")
col.put("p1", "Widget", indexes={"color": "red", "size": "large"})
col.put("p2", "Gadget", indexes={"color": "red", "size": "small"})

col.find_keys("color", "red")    # {"p1", "p2"}
col.find_keys("size", "large")   # {"p1"}

# Updating p1 with new indexes removes old entries automatically
col.put("p1", "Widget v2", indexes={"color": "blue"})
col.find_keys("color", "red")    # {"p2"}
col.find_keys("size", "large")   # set()
```
