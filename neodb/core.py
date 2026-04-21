class NeoDB:
    def __init__(self, dbname = "neodb"):
        self.dbname = dbname
        self.collections = {}
        self._collection_number = 0

    def collection(self, collection_name=None):
        if collection_name is None:
            self._collection_number += 1
            collection_name = f"Collection_{self._collection_number}"
        if collection_name not in self.collections:
            collection = NeoCollection(collection_name)
            self.collections[collection_name] = collection
            return collection
        return self.collections[collection_name]

    def list_collections(self) -> list:
        return list(self.collections.keys())

    def drop_collection(self, collection_name: str) -> bool:
        if collection_name in self.collections:
            del self.collections[collection_name]
            return True
        return False


class NeoCollection:
    def __init__(self, collection_name):
        self.name = collection_name
        self.records = dict()
        self.indexes = dict()
        self.reverse_indexes = dict()

    def get(self, key):
        record_value = self.records.get(key, None)
        return record_value

    def put(self, key, value, indexes=None):
        self._delete_indexes(key)
        self.records[key] = value
        if indexes:
            for index_name, index_value in indexes.items():
                self.indexes.setdefault(index_name, dict()).setdefault(index_value, set()).add(key)
                self.reverse_indexes.setdefault(key, dict())[index_name] = index_value

    def _delete_indexes(self, key):
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
        self._delete_indexes(key)
        return self.records.pop(key, None) is not None

    def find_keys(self, index_name, index_value):
        """
        Returns a set of keys matching the given index.
        Returns an empty set if no match is found.
        """
        # Using .get() ensures we return an empty set rather than a KeyError
        return self.indexes.get(index_name, {}).get(index_value, set())


    #
    # def _generate_id(self):
    #     with self._lock:
    #         now = time.time_ns()
    #         if now <= self._last_id:
    #             now = self._last_id + 1  # force monotonic increase
    #         self._last_id = now
    #         return now

