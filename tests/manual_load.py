import requests
from datetime import datetime

# TODO: locust keeps freezing - writing my own load test
# * create million buckets
# * delete buckets
# * store million documents
# * delete million documents
# * list random paths
# * store 1 GB document
# * try concurrency
# * collect stats


def timeit(f):
    def inn(*args):
        toc = datetime.now()
        result = f(*args)
        tic = datetime.now()
        print(tic-toc)
        return result
    return inn


class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._url = "http://" + host + ":" + port

    def create_bucket(self, bucket_url):
        response = requests.post(self._url + "/buckets" + bucket_url, timeout=10)
        return response.status_code == 200


client = Client("localhost", "8000")


@timeit
def create_buckets(top_count, sub_count):
    for x in range(top_count):
        client.create_bucket("/bucket-"+str(x))
        for y in range(sub_count):
            client.create_bucket("/bucket-"+str(x) + "/bucket" + str(y))


create_buckets(1000, 1000)
# 1000 top and 1000 sub for each crash docker, local 0:11:50.032247  102 MB
# 100 top 100 sub buckets: 0:00:10.607999
# 10 top 1000 sub 0:00:11.154776, 0:00:07.001230
# 100 top 1000 sub crash, 0:01:12.984563:102:MB memory usage (no docker)


