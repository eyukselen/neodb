class Database:
    def __init__(self, storage=None):
        self._storage = storage or InMemoryStorage()
        self._collections = {}

    def collection(self, name: str) -> "Collection":
        if name not in self._collections:
            self._collections[name] = Collection(name, self._storage)
        return self._collections[name]

    def __getattr__(self, name):
        return self.collection(name)


class Collection:
    def __init__(self, name: str, storage):
        self.name = name
        self._storage = storage

    def insert(self, doc: dict) -> dict:
        return self._storage.insert(self.name, doc)

    def get(self, **filters) -> dict | None:
        results = self.find(**filters)
        return results[0] if results else None

    def find(self, **filters) -> list[dict]:
        docs = self._storage.all(self.name)
        return QueryEngine.apply(docs, filters)

    def update(self, filters: dict, updates: dict) -> int:
        return self._storage.update(self.name, filters, updates)

    def delete(self, **filters) -> int:
        return self._storage.delete(self.name, filters)

    def __iter__(self):
        return iter(self._storage.all(self.name))

    def __len__(self):
        return len(self._storage.all(self.name))


class InMemoryStorage:
    def __init__(self):
        self._data = {}

    def _get_collection(self, name):
        return self._data.setdefault(name, [])

    def insert(self, name, doc):
        col = self._get_collection(name)

        doc = dict(doc)  # copy
        if "_id" not in doc:
            doc["_id"] = self._generate_id(col)

        col.append(doc)
        return doc

    def all(self, name):
        return list(self._get_collection(name))

    def update(self, name, filters, updates):
        col = self._get_collection(name)
        count = 0

        for doc in col:
            if QueryEngine.match(doc, filters):
                doc.update(updates)
                count += 1

        return count

    def delete(self, name, filters):
        col = self._get_collection(name)
        remaining = []
        count = 0

        for doc in col:
            if QueryEngine.match(doc, filters):
                count += 1
            else:
                remaining.append(doc)

        self._data[name] = remaining
        return count

    def _generate_id(self, col):
        return len(col) + 1


class QueryEngine:
    @staticmethod
    def apply(docs, filters):
        return [doc for doc in docs if QueryEngine.match(doc, filters)]

    @staticmethod
    def match(doc, filters):
        for key, value in filters.items():
            if "__" in key:
                field, op = key.split("__", 1)
                if not QueryEngine._compare(doc.get(field), op, value):
                    return False
            else:
                if doc.get(key) != value:
                    return False
        return True

    @staticmethod
    def _compare(field_value, op, value):
        if op == "gt":
            return field_value > value
        if op == "gte":
            return field_value >= value
        if op == "lt":
            return field_value < value
        if op == "lte":
            return field_value <= value
        if op == "ne":
            return field_value != value
        if op == "in":
            return field_value in value
        raise ValueError(f"Unsupported operator: {op}")


db = Database()

users = db.users

users.insert({"name": "Alice", "age": 30})
users.insert({"name": "Bob", "age": 20})

users.get(name="Alice")

users.find(age__gte=21)

users.update({"name": "Alice"}, {"age": 31})

users.delete(name="Bob")
