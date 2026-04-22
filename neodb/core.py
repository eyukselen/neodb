class NeoDB:
    """An in-memory database that manages named collections.

    Attributes:
        dbname: The name of the database.
        collections: A dictionary of collection names to NeoCollection instances.
    """

    def __init__(self, dbname="neodb"):
        """Initializes NeoDB with the given database name.

        Args:
            dbname: The name of the database. Defaults to "neodb".
        """
        self.dbname = dbname
        self.collections = {}
        self._collection_number = 0

    def collection(self, collection_name=None):
        """Gets or creates a collection by name.

        If no name is provided, an auto-generated name is used
        (e.g., "Collection_1", "Collection_2"). If a collection with the
        given name already exists, the existing instance is returned.

        Args:
            collection_name: The name of the collection. Defaults to None,
                which triggers auto-naming.

        Returns:
            The NeoCollection instance for the given name.
        """
        if collection_name is None:
            self._collection_number += 1
            collection_name = f"Collection_{self._collection_number}"
        if collection_name not in self.collections:
            collection = NeoCollection(collection_name)
            self.collections[collection_name] = collection
            return collection
        return self.collections[collection_name]

    def list_collections(self) -> list:
        """Lists all collection names in the database.

        Returns:
            A list of collection name strings.
        """
        return list(self.collections.keys())

    def drop_collection(self, collection_name: str) -> bool:
        """Drops a collection by name.

        Args:
            collection_name: The name of the collection to drop.

        Returns:
            True if the collection existed and was dropped, False otherwise.
        """
        if collection_name in self.collections:
            del self.collections[collection_name]
            return True
        return False


class NeoCollection:
    """A key-value store with optional indexing support.

    Records are stored as simple key-value pairs. Optional indexes allow
    fast lookups by index name and value. A reverse index is maintained
    for efficient cleanup on delete or update.

    Attributes:
        name: The name of the collection.
        records: A dictionary mapping keys to their values.
        indexes: A forward index mapping
            ``{index_name: {index_value: {keys}}}``.
        reverse_indexes: A reverse index mapping
            ``{key: {index_name: index_value}}``.
    """

    def __init__(self, collection_name):
        """Initializes a NeoCollection with the given name.

        Args:
            collection_name: The name of the collection.
        """
        self.name = collection_name
        self.records = dict()
        self.indexes = dict()
        self.reverse_indexes = dict()

    def get(self, key):
        """Retrieves the value for a given key.

        Args:
            key: The key to look up.

        Returns:
            The stored value, or None if the key does not exist.
        """
        record_value = self.records.get(key, None)
        return record_value

    def put(self, key, value, indexes=None):
        """Stores a value under the given key with optional indexes.

        If the key already exists, the old value and its indexes are
        replaced. Index entries are automatically maintained.

        Args:
            key: The key to store the value under.
            value: The value to store.
            indexes: An optional dictionary of ``{index_name: index_value}``
                pairs for indexing the record. Defaults to None.
        """
        self._delete_indexes(key)
        self.records[key] = value
        if indexes:
            for index_name, index_value in indexes.items():
                self.indexes.setdefault(index_name, dict()).setdefault(index_value, set()).add(key)
                self.reverse_indexes.setdefault(key, dict())[index_name] = index_value

    def _delete_indexes(self, key):
        """Removes all index entries associated with a key.

        Uses the reverse index to find and clean up forward index entries.
        Empty index buckets are removed to keep the index structure clean.

        Args:
            key: The key whose index entries should be removed.
        """
        index_to_remove = self.reverse_indexes.pop(key, None)
        if index_to_remove:
            for index_name, index_value in index_to_remove.items():
                if index_name in self.indexes and index_value in self.indexes[index_name]:
                    self.indexes[index_name][index_value].discard(key)
                    if not self.indexes[index_name][index_value]:
                        del self.indexes[index_name][index_value]
                    if not self.indexes[index_name]:
                        del self.indexes[index_name]

    def delete(self, key):
        """Deletes a record and cleans up its indexes.

        Args:
            key: The key to delete.

        Returns:
            True if the key existed and was deleted, False otherwise.
        """
        self._delete_indexes(key)
        return self.records.pop(key, None) is not None

    def find_keys(self, index_name, index_value):
        """Finds all keys matching a given index name and value.

        Args:
            index_name: The name of the index to search.
            index_value: The index value to match.

        Returns:
            A set of keys matching the index, or an empty set if no
            match is found.
        """
        return self.indexes.get(index_name, {}).get(index_value, set())
