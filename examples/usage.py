from eprofiler import profile
from neodb import NeoDB


def test_neocollection(insert_count=1000):
    neodb = NeoDB()
    coll = neodb.collection("NeoCollection")
    for x in range(insert_count):
        key = f"key-{x}"
        data = f"data-{x}"
        index = { f"index-{a}": f"index-{a}_value-{a}" for a in range(10)}
        coll.put(key, data, index)

profiled_neocollection = profile(test_neocollection, label="NeoCollection")
profiled_neocollection(insert_count=1000)
