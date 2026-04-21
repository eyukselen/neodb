from neodb import NeoDB
from neodb.core import NeoCollection
from pprint import pprint
from eprofiler import memit, profile_cpu, profile
import random
import string


# region NeoDB base ops
ndb = NeoDB("test db")
collection = ndb.collection()

print(ndb.list_collections())

for collection_name in ndb.list_collections():
    ndb.drop_collection(collection_name)

# endregion

# region simple ops

def test_collection(collection, collection_name, insert_count=1000):
    coll = collection(collection_name)
    for x in range(insert_count):
        key = f"key-{x}"
        data = f"data-{x}"
        index = { f"index-{a}": f"index-{a}_value-{a}" for a in range(10)}
        # unique_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        # index_name = f"attr_{unique_suffix}_{x}"
        # index_value = f"value_{x}"
        # index = {index_name: index_value}
        coll.put(key, data)

profiled_test = profile(test_collection, label="AdHoc_Test")

profiled_test(NeoCollection, "NeoCollection2", insert_count=100_000)






key = "key-0"
data = "data-0"
index_name = "index-1"
index_value = "index_value-0"
index = {index_name: index_value}

data = {key: (data, {index_name: index_value})}
index = {index_name:{index_value: {key}}}

records = {key: data}
index = {index_name:{index_value: {key}}}
cleanup_index =  {key:{index_name: index_value}}

exit()

print("====")
for k, v in collection.index_data.items():
    print(k, v)


for x in range(5, 10, 1):
    collection.delete(f"key-{x}")

print("====")
for k, v in collection.index_data.items():
    print(k, v)


# endregion
