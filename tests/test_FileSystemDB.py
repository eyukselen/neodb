from unittest import TestCase
from src.neodb.FileSystemDB import FileSystemDB
import os
import shutil

BASE_PATH = os.path.abspath(os.path.dirname(__file__)) + '/db_folder'
print(BASE_PATH)


def create_file(file_path, contents):
    with open(file_path, 'w') as file:
        file.write(contents)


# TODO: need to add tests for sub folders
class Test(TestCase):
    def test_bucket_exists(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket1'
        self.assertFalse(service.bucket_exists(bucket_url))
        os.makedirs(BASE_PATH + bucket_url, exist_ok=False)
        self.assertTrue(service.bucket_exists(bucket_url))
        self.assertTrue(os.path.isdir(BASE_PATH + bucket_url))
        # # cleanup
        os.rmdir(BASE_PATH + bucket_url)

    def test_list_buckets(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket2'
        os.makedirs(BASE_PATH + bucket_url, exist_ok=False)
        buckets = ['/bucket2/data1', '/bucket2/data2', '/bucket2/data3']
        for bucket in buckets:
            os.makedirs(BASE_PATH + bucket)
        self.assertEqual(sorted(buckets), sorted(service.list_buckets(bucket_url)))
        list_false_bucket = service.list_buckets("/not_existing_bucket")
        self.assertFalse(list_false_bucket)
        # cleanup
        shutil.rmtree(BASE_PATH + bucket_url)

    def test_create_bucket(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket3'
        bucket_created = service.create_bucket(bucket_url)
        self.assertTrue(bucket_created)
        self.assertTrue(os.path.isdir(BASE_PATH + bucket_url))
        bucket_created_again = service.create_bucket(bucket_url)
        self.assertFalse(bucket_created_again)
        self.assertTrue(os.path.isdir(BASE_PATH + bucket_url))
        # cleanup
        os.rmdir(BASE_PATH + bucket_url)

    def test_delete_bucket(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket4'
        os.makedirs(BASE_PATH + bucket_url)
        bucket_deleted = service.delete_bucket(bucket_url)
        self.assertTrue(bucket_deleted)
        bucket_deleted_again = service.delete_bucket(bucket_url)
        self.assertFalse(bucket_deleted_again)

    def test_document_exists(self):
        service = FileSystemDB(BASE_PATH)
        document_url = "/bucket5/document.txt"
        bucket_url = "/bucket5"
        document_exists = service.document_exists(document_url)
        self.assertFalse(document_exists)
        os.makedirs(BASE_PATH + bucket_url)
        create_file(BASE_PATH + document_url, "Some data")
        document_exists_now = service.document_exists(document_url)
        self.assertTrue(document_exists_now)
        # cleanup
        shutil.rmtree(BASE_PATH + bucket_url)  # !careful not to delete wrong folder from root

    def test_list_documents(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket8'
        os.makedirs(BASE_PATH + bucket_url, exist_ok=False)
        documents = ['/bucket8/document1.txt', '/bucket8/document2.txt', '/bucket8/document3.txt']
        for document in documents:
            create_file(BASE_PATH + document, "some data")
        self.assertEqual(sorted(documents), sorted(service.list_documents(bucket_url)))
        list_false_bucket = service.list_documents("/not_existing_bucket")
        self.assertFalse(list_false_bucket)
        # cleanup
        shutil.rmtree(BASE_PATH + bucket_url)

    def test_read_document(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket5'
        os.makedirs(BASE_PATH + bucket_url)
        document_url = bucket_url + "/document1.txt"
        service.create_bucket(bucket_url)
        contents = "Some data"
        create_file(BASE_PATH + document_url, contents)
        data = service.read_document(document_url)
        self.assertEqual(data, contents)
        read_false_document = service.read_document("/non_existed_document")
        self.assertFalse(read_false_document)
        # cleanup
        shutil.rmtree(BASE_PATH + bucket_url)  # !careful not to delete wrong folder from root

    def test_store_document(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket6'
        os.makedirs(BASE_PATH + bucket_url)
        document_url = bucket_url + '/document2.txt'
        contents = "random data"
        document_stored = service.store_document(document_url, contents)
        self.assertTrue(document_stored)
        with open(BASE_PATH + document_url, 'r') as file:
            data = file.read()
        self.assertEqual(data, contents)
        # cleanup
        shutil.rmtree(BASE_PATH + bucket_url)

    def test_delete_document(self):
        service = FileSystemDB(BASE_PATH)
        bucket_url = '/bucket7'
        os.makedirs(BASE_PATH + bucket_url)
        document_url = bucket_url + '/document7.txt'
        contents = "some data"
        delete_false_document = service.delete_document(document_url)
        self.assertFalse(delete_false_document)
        create_file(BASE_PATH + document_url, contents)
        delete_true_document = service.delete_document(document_url)
        self.assertTrue(delete_true_document)
        # cleanup
        os.rmdir(BASE_PATH + bucket_url)

