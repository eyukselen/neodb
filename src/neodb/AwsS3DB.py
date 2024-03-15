from neodb.BackEnd import StorageBackend
from typing import Any, Union
import boto3
import botocore
from botocore.exceptions import ClientError


class AwsS3DB(StorageBackend):
    def __init__(self, base_path, profile_name=None):
        self.base_path = base_path
        self.profile_name = profile_name
        self.session = boto3.Session(profile_name=self.profile_name)
        self.s3 = self.session.client('s3')

    @staticmethod
    def _make_url(s3_url):
        if not s3_url.endswith('/'):
            s3_url += '/'
        if s3_url.startswith('/'):
            s3_url = s3_url[1:]
        return s3_url

    def bucket_exists(self, bucket_url: str) -> bool:
        bucket_url = self._make_url(bucket_url)

        response = self.s3.list_objects_v2(
            Bucket=self.base_path,
            Prefix=bucket_url,
            Delimiter='/'
        )
        return 'Contents' in response or 'CommonPrefixes' in response

    def list_buckets(self, bucket_url: str) -> Union[list, bool]:
        # TODO: need to add pagination for long lists
        bucket_url = self._make_url(bucket_url)

        response = self.s3.list_objects_v2(
            Bucket=self.base_path,
            Prefix=bucket_url,
            Delimiter='/'
        )
        if response["KeyCount"] != 0:
            folders = [obj["Prefix"].rstrip('/') for obj in response["CommonPrefixes"]]
            return folders
        return []

    def create_bucket(self, bucket_url: str) -> bool:
        bucket_url = self._make_url(bucket_url)
        if self.bucket_exists(bucket_url):
            return False

        response = self.s3.put_object(Bucket=self.base_path, Key=bucket_url)
        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def delete_bucket(self, bucket_url: str) -> bool:
        bucket_url = self._make_url(bucket_url)

        def delete_s3_prefix(bucket_name, prefix):

            # List all objects in the specified prefix
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            # Extract keys from response
            objects = [{'Key': obj['Key']} for obj in response.get('Contents', [])]

            # If there are objects, delete them
            if objects:
                self.s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})

            # If there are more objects to delete (due to pagination), recursively call the function
            # TODO: need to add NextContinuationToken for test coverage
            if 'NextContinuationToken' in response:
                delete_s3_prefix(bucket_name, prefix)

            # Delete the empty "folder" (prefix)
            self.s3.delete_object(Bucket=bucket_name, Key=prefix)

        delete_s3_prefix(self.base_path, bucket_url)
        return True

    def document_exists(self, document_url) -> bool:
        document_url = self._make_url(document_url).rstrip("/")
        folder = "/".join(document_url.split("/")[:-1]) + "/"
        doc_name = "".join(document_url.split("/")[-1:])
        try:
            self.s3.head_object(Bucket=self.base_path, Key=document_url)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False

    def list_documents(self, bucket_url: str) -> Union[list, bool]:
        bucket_url = self._make_url(bucket_url)

        response = self.s3.list_objects_v2(
            Bucket=self.base_path,
            Prefix=bucket_url,
            Delimiter='/'
        )
        # TODO: response["KeyCount"] needs to change - not always in response top level
        if response["KeyCount"] != 0:
            documents = ["/" + obj["Key"] for obj in response["Contents"]]
            documents.remove("/" + bucket_url)
            return documents
        return False

    def read_document(self, document_url: str) -> Any:
        document_url = self._make_url(document_url).rstrip('/')
        try:
            response = self.s3.get_object(Bucket=self.base_path, Key=document_url)
            fetched = response['Body'].read()
            return fetched
        except botocore.exceptions.ClientError as e:
            return False

    def store_document(self, document_url: str, document: Any) -> bool:
        """
        stores a document in s3 bucket

        :param document_url:
        :param document: need to be text not bytes
        :return:
        """
        object_name = ''.join(document_url.split('/')[-1:])
        bucket_url = '/'.join(document_url.split('/')[:-1])
        sub_folder = self._make_url(bucket_url)
        key = sub_folder + object_name
        document = document.encode('utf-8')
        try:
            response = self.s3.put_object(Bucket=self.base_path, Key=key, Body=document)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True
        except botocore.exceptions.ClientError as e:
            return False

    def delete_document(self, document_url: str) -> bool:
        document_url = self._make_url(document_url).rstrip("/")
        res = self.s3.head_object(Bucket=self.base_path, Key=document_url)
        if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
            response = self.s3.delete_object(Bucket=self.base_path, Key=document_url)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                return True
        return False
