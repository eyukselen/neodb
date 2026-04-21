from neodb.core import NeoDB, NeoCollection


class TestNeoDB:

    def test_default_dbname(self):
        db = NeoDB()
        assert db.dbname == "neodb"

    def test_custom_dbname(self):
        db = NeoDB("mydb")
        assert db.dbname == "mydb"

    def test_collection_named(self):
        db = NeoDB()
        col = db.collection("users")
        assert isinstance(col, NeoCollection)
        assert col.name == "users"

    def test_collection_returns_existing(self):
        db = NeoDB()
        col1 = db.collection("users")
        col2 = db.collection("users")
        assert col1 is col2

    def test_collection_auto_named(self):
        db = NeoDB()
        col1 = db.collection()
        col2 = db.collection()
        assert col1.name == "Collection_1"
        assert col2.name == "Collection_2"

    def test_list_collections_empty(self):
        db = NeoDB()
        assert db.list_collections() == []

    def test_list_collections(self):
        db = NeoDB()
        db.collection("a")
        db.collection("b")
        assert sorted(db.list_collections()) == ["a", "b"]

    def test_drop_collection_existing(self):
        db = NeoDB()
        db.collection("users")
        assert db.drop_collection("users") is True
        assert "users" not in db.list_collections()

    def test_drop_collection_nonexistent(self):
        db = NeoDB()
        assert db.drop_collection("nope") is False


class TestNeoCollection:

    def test_put_and_get(self):
        col = NeoCollection("test")
        col.put("k1", {"name": "Alice"})
        assert col.get("k1") == {"name": "Alice"}

    def test_get_missing_key(self):
        col = NeoCollection("test")
        assert col.get("missing") is None

    def test_put_overwrites(self):
        col = NeoCollection("test")
        col.put("k1", "v1")
        col.put("k1", "v2")
        assert col.get("k1") == "v2"

    def test_delete_existing(self):
        col = NeoCollection("test")
        col.put("k1", "v1")
        assert col.delete("k1") is True
        assert col.get("k1") is None

    def test_delete_missing(self):
        col = NeoCollection("test")
        assert col.delete("missing") is False

    def test_put_with_indexes(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        assert "color" in col.indexes
        assert "red" in col.indexes["color"]
        assert "k1" in col.indexes["color"]["red"]

    def test_put_multiple_keys_same_index(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.put("k2", "v2", indexes={"color": "red"})
        assert col.indexes["color"]["red"] == {"k1", "k2"}

    def test_overwrite_updates_indexes(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.put("k1", "v2", indexes={"color": "blue"})
        assert "k1" not in col.indexes.get("color", {}).get("red", set())
        assert "k1" in col.indexes["color"]["blue"]

    def test_delete_cleans_indexes(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.delete("k1")
        assert col.indexes == {}
        assert col.reverse_indexes == {}

    def test_delete_partial_index_cleanup(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.put("k2", "v2", indexes={"color": "red"})
        col.delete("k1")
        assert col.indexes["color"]["red"] == {"k2"}

    def test_put_with_multiple_indexes(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red", "size": "large"})
        assert "k1" in col.indexes["color"]["red"]
        assert "k1" in col.indexes["size"]["large"]

    def test_overwrite_removes_old_indexes_completely(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.put("k1", "v2")
        assert col.indexes == {}
        assert col.reverse_indexes == {}

    def test_reverse_indexes_tracked(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red", "size": "large"})
        assert col.reverse_indexes["k1"] == {"color": "red", "size": "large"}

    def test_reverse_indexes_updated_on_overwrite(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.put("k1", "v2", indexes={"color": "blue"})
        assert col.reverse_indexes["k1"] == {"color": "blue"}

    def test_find_keys_match(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.put("k2", "v2", indexes={"color": "red"})
        assert col.find_keys("color", "red") == {"k1", "k2"}

    def test_find_keys_no_match(self):
        col = NeoCollection("test")
        assert col.find_keys("color", "red") == set()

    def test_find_keys_missing_index_name(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        assert col.find_keys("size", "large") == set()

    def test_find_keys_missing_index_value(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        assert col.find_keys("color", "blue") == set()

    def test_find_keys_after_delete(self):
        col = NeoCollection("test")
        col.put("k1", "v1", indexes={"color": "red"})
        col.delete("k1")
        assert col.find_keys("color", "red") == set()

    def test_put_no_return_value(self):
        col = NeoCollection("test")
        assert col.put("k1", "v1") is None
