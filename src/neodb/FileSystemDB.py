import os
from neodb.BackEnd import StorageBackend
from typing import Any, Union
import shutil


class FileSystemDB(StorageBackend):
    # TODO: need to handle paths if it is windows
    def __init__(self, base_path):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    def bucket_exists(self, bucket_url: str) -> bool:
        return os.path.isdir(self.base_path + bucket_url)

    def list_buckets(self, bucket_url: str) -> Union[list, bool]:
        bucket_path = self.base_path + bucket_url
        if os.path.isdir(bucket_path):
            return ['/'.join([bucket_url, item]) for item in os.listdir(bucket_path)
                    if os.path.isdir(os.path.join(bucket_path, item))]
        return False

    def create_bucket(self, bucket_url: str) -> bool:
        if os.path.isdir(self.base_path + bucket_url):
            return False
        os.makedirs(self.base_path + bucket_url, exist_ok=False)
        from time import sleep
        sleep(1)
        return True

    def delete_bucket(self, bucket_url: str) -> bool:
        if os.path.isdir(self.base_path + bucket_url):
            shutil.rmtree(self.base_path + bucket_url)
            return True
        return False

    def document_exists(self, document_url) -> bool:
        if os.path.isfile(self.base_path + document_url):
            return True
        return False

    def list_documents(self, bucket_url: str) -> Union[list, bool]:
        bucket_path = self.base_path + bucket_url
        if os.path.isdir(bucket_path):
            return ['/'.join([bucket_url, item]) for item in os.listdir(bucket_path)
                    if os.path.isfile(os.path.join(bucket_path, item))]
        return False

    def read_document(self, document_url: str) -> Any:
        # TODO: need a mechanism for very large files
        # TODO: should read binary or text? for now text
        if os.path.isfile(self.base_path + document_url):
            with open(self.base_path + document_url, 'r') as file:
                res = file.read()
            return res
        return False

    def store_document(self, document_url: str, document: Any) -> bool:
        with open(self.base_path + document_url, 'w') as file:
            file.write(document)
        return True

    def delete_document(self, document_url: str) -> bool:
        if os.path.isfile(self.base_path + document_url):
            os.remove(self.base_path + document_url)
            return True
        return False
