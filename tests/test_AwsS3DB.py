from unittest import TestCase, mock
from moto import mock_aws
from src.neodb.AwsS3DB import AwsS3DB
import boto3
import os
import io

os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
os.environ['AWS_SECURITY_TOKEN'] = 'testing'
os.environ['AWS_SESSION_TOKEN'] = 'testing'
os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'


class TestAwsS3DB(TestCase):
    def setUp(self):
        self.base_path = 'my_db_bucket'
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        create_bucket_configuration = {
            'LocationConstraint': 'eu-west-2'
        }

        self.session = boto3.Session()
        self.s3 = self.session.client('s3')
        self.s3.create_bucket(Bucket=self.base_path,
                              CreateBucketConfiguration=create_bucket_configuration)
        self.s3db = AwsS3DB(self.base_path)

    def test_bucket_exists(self):
        db_bucket = "my-bucket-1"
        # create a folder
        res = self.s3db.create_bucket("/" + db_bucket)
        self.assertEqual(res, True)
        # check if it exists with boto
        response = self.s3.list_objects_v2(Bucket=self.base_path, Prefix=db_bucket + "/")
        self.assertTrue('Contents' in response)
        # check if it exists with s3db
        resp = self.s3db.bucket_exists("/" + db_bucket)
        self.assertEqual(resp, True)

    def test_list_buckets(self):
        db_bucket_1 = "my-bucket-1"
        db_bucket_2 = "my-bucket-2"
        response = self.s3.put_object(Bucket=self.base_path, Key=db_bucket_1 + "/")
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        response = self.s3.put_object(Bucket=self.base_path, Key=db_bucket_2 + "/")
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        res = self.s3db.list_buckets("/")
        self.assertEqual(sorted(res), sorted([db_bucket_1, db_bucket_2]))
        # list non-existing bucket
        res = self.s3db.list_buckets("/not-a-bucket")
        self.assertEqual(res, [])

    def test_create_bucket(self):
        res = self.s3db.create_bucket("/my-bucket-1")
        self.assertEqual(res, True)
        # return false when creating an existing bucket
        res = self.s3db.create_bucket("/my-bucket-1")
        self.assertFalse(res)

    def test_delete_bucket(self):
        # create some folders
        self.s3.put_object(Bucket=self.base_path, Key="bucket-1")
        self.s3.put_object(Bucket=self.base_path, Key="bucket-1/bucket-1.1")
        self.s3.put_object(Bucket=self.base_path, Key="bucket-1/bucket-1.2")
        # create some docs
        docs = ["bucket-1/bucket-1.1/document-1.txt",
                "bucket-1/bucket-1.1/document-2.txt",
                "bucket-1/bucket-1.2/document-3.txt",
                "bucket-1/bucket-1.2/document-4.txt",
                ]
        doc_data = "test data!"
        #
        for x in range(1002):
            doc_url = f"bucket-1/bucket-1.1/document-{x}.txt"
            res = self.s3.put_object(Bucket=self.base_path, Key=doc_url, Body=doc_data)
        res = self.s3db.delete_bucket("/bucket-1")
        self.assertTrue(res)
        for doc_url in docs:
            res = self.s3db.document_exists("/" + doc_url)
            self.assertEqual(res, False)

    def test_document_exists(self):
        db_bucket_1 = "my-bucket-1"
        doc_url = db_bucket_1 + "/" + "my-document.txt"
        doc_data = "test data!"
        self.s3.put_object(Bucket=self.base_path, Key=db_bucket_1 + "/")
        res = self.s3db.store_document("/" + doc_url, doc_data)
        self.assertEqual(res, True)
        response = self.s3.get_object(Bucket=self.base_path, Key=doc_url)
        self.assertTrue(response)
        result = self.s3db.document_exists("/" + doc_url)
        self.assertEqual(result, True)

    def test_list_documents(self):
        db_bucket_1 = "my-bucket-1"
        doc_data = "test data!"
        # create folder
        self.s3.put_object(Bucket=self.base_path, Key=db_bucket_1 + "/")
        expected = []
        for x in range(10):
            doc_url = db_bucket_1 + "/" + "my-document-" + str(x) + ".txt"
            expected.append("/" + doc_url)
            self.s3.put_object(Bucket=self.base_path, Key=doc_url, Body=doc_data)
        res = self.s3db.list_documents("/" + db_bucket_1)
        self.assertEqual(sorted(res), sorted(expected))
        # list documents for non-existing bucket
        res = self.s3db.list_documents("/not-a-bucket")
        self.assertFalse(res)

    def test_read_document(self):
        db_bucket_1 = "my-bucket-1"
        doc_url = db_bucket_1 + "/" + "my-document.txt"
        doc_data = "test data!"
        # response = self.s3.put_object(Bucket=self.base_path, Key=db_bucket_1 + "/")
        res = self.s3db.store_document("/" + doc_url, doc_data)
        self.assertEqual(res, True)
        # response = self.s3.get_object(Bucket=self.base_path, Key=doc_url)
        fetched = self.s3db.read_document("/" + doc_url)
        # fetched = response['Body'].read()
        self.assertEqual(fetched, doc_data.encode('utf-8'))
        # read non-existing document
        fetched = self.s3db.read_document("/not-a-bucket/not-a-document.txt")
        self.assertFalse(fetched)

    def test_store_document(self):
        db_bucket_1 = "my-bucket-1"
        doc_url = db_bucket_1 + "/" + "my-document.txt"
        doc_data = "test data!"
        self.s3.put_object(Bucket=self.base_path, Key=db_bucket_1)
        res = self.s3db.store_document("/" + doc_url, doc_data)
        self.assertEqual(res, True)
        # store document
        response = self.s3.get_object(Bucket=self.base_path, Key=doc_url)
        fetched = response['Body'].read()
        self.assertEqual(fetched, doc_data.encode('utf-8'))
        # store wrong aws bucket
        s3db_fake = AwsS3DB("None")
        response = s3db_fake.store_document("/" + doc_url, doc_data)
        self.assertFalse(response)

    def test_store_large_document(self):
        db_bucket_1 = "my-bucket-1"
        doc_url = db_bucket_1 + "/" + "my-document.txt"
        self.s3.put_object(Bucket=self.base_path, Key=db_bucket_1)
        data_stream = io.BytesIO(bytearray(b'#' * (10 * 1024 * 1024)))
        result = self.s3db.store_large_document(doc_url, data_stream, chunk_size=1024 * 1024 * 5)
        self.assertTrue(result)
        with mock.patch.object(self.s3, 'upload_part') as mock_upload_part:
            result = self.s3db.store_large_document(doc_url, data_stream, chunk_size=1024 * 1024 * 5)
            mock_upload_part.side_effect = Exception("Test exception")
        self.assertFalse(result)

    def test_delete_document(self):
        db_bucket_1 = "my-bucket-1"
        doc_url = db_bucket_1 + "/" + "my-document.txt"
        doc_data = "test data!"
        # create folder
        response = self.s3.put_object(Bucket=self.base_path, Key=db_bucket_1 + "/")
        self.assertEqual(200, response["ResponseMetadata"]["HTTPStatusCode"])
        # store a document
        response = self.s3.put_object(Bucket=self.base_path, Key=doc_url, Body=doc_data)
        self.assertEqual(200, response["ResponseMetadata"]["HTTPStatusCode"])
        # check it exists
        r1 = self.s3db.document_exists("/" + doc_url)
        self.assertTrue(r1)
        # delete document
        res = self.s3db.delete_document("/" + doc_url)
        self.assertEqual(res, True)
        # check it is deleted
        r2 = self.s3db.document_exists("/" + doc_url)
        self.assertFalse(r2)
        # delete non-existing document
        r3 = self.s3db.delete_document("/dummy.txt")
        self.assertFalse(r3)

    def tearDown(self):
        self.mock_aws.stop()
