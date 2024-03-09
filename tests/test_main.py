from unittest import TestCase
from fastapi.testclient import TestClient
from neodb.main import server
import io


# test functions are taken out to be reusable for load testing
def list_buckets(client, bucket_url):
    return client.get("http://127.0.0.1:8000/buckets" + bucket_url)


def create_bucket(client, bucket_url):
    return client.post("http://127.0.0.1:8000/buckets" + bucket_url)


def delete_bucket(client, bucket_url):
    return client.delete("http://127.0.0.1:8000/buckets" + bucket_url)


def list_documents(client, bucket_url):
    return client.get("http://127.0.0.1:8000/documents" + bucket_url)


def get_document(client, document_url):
    return client.get("http://127.0.0.1:8000/document" + document_url)


def store_document(client, document_url, data):
    return client.post("/document/bucket4/document1.txt",
                       files={"file": data},
                       # headers={"Content-Type": "text/plain"}
                       )


def delete_document(client, document_url):
    return client.delete("http://127.0.0.1:8000/document" + document_url)


class Test(TestCase):
    def test_root(self):
        app = server(backend='memory')
        client = TestClient(app)
        response = client.get("http://127.0.0.1:8000/")
        assert response.status_code == 200
        assert response.json() == {"message": "Mock Document DB"}

    def test_list_buckets(self):
        app = server(backend='memory')
        client = TestClient(app)
        # non existed bucket
        response = list_buckets(client, "/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])
        # create some folders
        response = create_bucket(client, "/bucket1")
        self.assertEqual(response.status_code, 200)
        response = create_bucket(client, "/bucket1/bucket2")
        self.assertEqual(response.status_code, 200)
        response = create_bucket(client, "/bucket1/bucket3")
        self.assertEqual(response.status_code, 200)
        # existing buckets
        response = list_buckets(client, "/bucket1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), ['/bucket1/bucket2', '/bucket1/bucket3'])

    def test_create_bucket(self):
        app = server(backend='memory')
        client = TestClient(app)
        bucket_url = "/bucket1"
        response = create_bucket(client, bucket_url)
        self.assertEqual(response.status_code, 200)
        response = list_buckets(client, "/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [bucket_url])
        # try to create existing bucket
        response = create_bucket(client, bucket_url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'message': "bucket cannot be created:" + bucket_url})

    def test_delete_bucket(self):
        app = server(backend='memory')
        client = TestClient(app)
        # first create a bucket to delete
        response = create_bucket(client, "/bucket2")
        self.assertEqual(response.status_code, 200)
        # check it exists
        response = list_buckets(client, "/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), ['/bucket2'])
        # delete the bucket
        delete_bucket(client, "/bucket2")
        self.assertEqual(response.status_code, 200)
        # check it does not exist
        response = list_buckets(client, "/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])

    def test_list_documents(self):
        app = server(backend='memory')
        client = TestClient(app)
        # this needs a bucket exist
        response = create_bucket(client, "/bucket4")
        self.assertEqual(response.status_code, 200)
        # check bucket exists
        response = list_buckets(client, "/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), ['/bucket4'])
        # store a document in the bucket created
        file_data = b"This is the content of the string."
        string_io = io.BytesIO(file_data)
        response = store_document(client, "/bucket4/document1.txt", string_io)
        assert response.status_code == 200
        # list documents to get the document stored in return
        response = list_documents(client, "/bucket4")
        self.assertEqual(response.json(), ["/bucket4/document1.txt"])
        self.assertEqual(response.status_code, 200)
        # list documents for non existed folder
        response = list_documents(client, "/false_bucket")
        self.assertEqual(response.status_code, 404)

    def test_get_document(self):
        app = server(backend='memory')
        client = TestClient(app)
        # this needs a bucket exist
        response = create_bucket(client, "/bucket4")
        self.assertEqual(response.status_code, 200)
        # check it exists
        response = list_buckets(client, "/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), ['/bucket4'])
        # store a document in the bucket created
        file_data = b"This is the content of the string."
        string_io = io.BytesIO(file_data)
        response = store_document(client, "/bucket4/document1.txt", string_io)
        assert response.status_code == 200
        # get the document stored
        response = get_document(client, "/bucket4/document1.txt")
        self.assertEqual(response.status_code, 200)

    def test_store_document(self):
        app = server(backend='memory')
        client = TestClient(app)
        # this needs a bucket exist
        response = create_bucket(client, "/bucket4")
        assert response.status_code == 200
        # check if bucket created
        response = list_buckets(client, "/")
        assert response.status_code == 200
        assert response.json() == ["/bucket4"]
        # store a document in the bucket created
        file_data = b"This is the content of the string."
        string_io = io.BytesIO(file_data)
        response = store_document(client, "/bucket4/document.txt", string_io)
        assert response.status_code == 200

    def test_delete_document(self):
        app = server(backend='memory')
        client = TestClient(app)
        # this needs a bucket exist
        response = create_bucket(client, "/bucket4")
        assert response.status_code == 200
        # check if bucket created
        response = list_buckets(client, "/")
        assert response.status_code == 200
        assert response.json() == ["/bucket4"]
        # store a document in the bucket created
        file_data = b"This is the content of the string."
        string_io = io.BytesIO(file_data)
        store_document(client, "/bucket4/document.txt", string_io)
        assert response.status_code == 200
        # delete non existed document
        response = client.delete("http://127.0.0.1:8000/document/bucket4/document_false.txt")
        assert response.status_code == 200
        # delete document
        response = delete_document(client, "/bucket4/document.txt")
        assert response.status_code == 200
