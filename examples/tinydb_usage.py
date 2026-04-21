from tinydb import TinyDB, Query

tdb = TinyDB("test.json")

for x in range(100):
    doc = {"id": x, "name": "emre", "age": 25, "city": "istanbul"}
    tdb.insert(doc)
    tdb.insert()


print(tdb.all())


