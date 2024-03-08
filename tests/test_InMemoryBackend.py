from unittest import TestCase
from InMemoryBackend import InMemoryBackend


def create_bucket(service, bucket_url='/bucket1'):
    service.create_bucket(bucket_url)


def delete_bucket(service, bucket_url='/bucket1'):
    service.delete_bucket(bucket_url)


def store_document(service, document_url, document):
    service.store_document(document_url, document)


# TODO: need to add tests for sub folders
class Test(TestCase):
    def test_bucket_exists(self):
        service = InMemoryBackend()
        bucket_url = '/bucket1'
        self.assertFalse(service.bucket_exists(bucket_url))
        create_bucket(service, bucket_url)
        self.assertTrue(service.bucket_exists(bucket_url))
        # cleanup
        delete_bucket(service, bucket_url)

    def test_list_buckets(self):
        service = InMemoryBackend()
        bucket_url = '/bucket1'
        create_bucket(service, bucket_url)
        buckets = ['/bucket1/bucket11', '/bucket1/bucket22']
        for bucket in buckets:
            create_bucket(service, bucket)
        self.assertEqual(service.list_buckets(bucket_url), buckets)
        self.assertFalse(service.list_buckets("/not_exist_bucket"))

    def test_create_bucket(self):
        service = InMemoryBackend()
        bucket_url = '/bucket2'
        self.assertTrue(service.create_bucket(bucket_url))
        self.assertTrue(bucket_url in service.buckets)
        # bucket already exist should return False
        self.assertFalse(service.create_bucket(bucket_url))

    def test_delete_bucket(self):
        service = InMemoryBackend()
        bucket_url = '/bucket3'
        create_bucket(service, bucket_url)
        store_document(service, bucket_url + "/document1.txt", "some data")
        self.assertTrue(service.delete_bucket(bucket_url))
        self.assertFalse(bucket_url in service.buckets)
        self.assertFalse(service.delete_bucket("/not_existed_bucket"))

    def test_document_exist(self):
        service = InMemoryBackend()
        bucket_url = "/bucket_1"
        create_bucket(service, bucket_url)
        document = "some random data"
        document_url = "/bucket_1/document1.txt"
        store_document(service, document_url, document)
        self.assertTrue(service.document_exists(document_url))
        no_document_url = "/bucket_1/document2.txt"
        self.assertFalse(service.document_exists(no_document_url))

    def test_list_documents(self):
        service = InMemoryBackend()
        bucket_url = "/bucket_1"
        create_bucket(service, bucket_url)
        document = "some random data"
        document_url = "/bucket_1/document1.txt"
        store_document(service, document_url, document)
        self.assertEqual(service.list_documents(bucket_url), [document_url])
        self.assertFalse(service.list_documents("/non_existed_bucket"))

    def test_read_document(self):
        service = InMemoryBackend()
        bucket_url = '/bucket4'
        create_bucket(service, bucket_url)
        document_url = bucket_url + '/document1'
        service.store_document(document_url, "random data")
        data = service.read_document(document_url)
        self.assertEqual(data, "random data")

    def test_store_document(self):
        service = InMemoryBackend()
        bucket_url = '/bucket2'
        document = bucket_url + '/document2'
        service.create_bucket(bucket_url)
        service.store_document(document, "random data")
        data = service.read_document(document)
        self.assertEqual(data, "random data")
        self.assertFalse(service.store_document("/non_existing_bucket/document1/txt", "Some data"))

    def test_delete_document(self):
        service = InMemoryBackend()
        bucket_url = '/bucket3'
        document = bucket_url + '/document3'
        service.create_bucket(bucket_url)
        service.store_document(document, "random data")
        data = service.read_document(document)
        self.assertEqual(data, "random data")
        self.assertTrue(service.delete_document(document))
        self.assertFalse(service.read_document(document))
        self.assertFalse(service.delete_document("/non_existing_bucket/document1.txt"))
