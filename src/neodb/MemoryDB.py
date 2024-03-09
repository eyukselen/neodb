"""
MemoryDB is a simple key value store using python dictionaries
"""
from typing import Any, Union, Optional
from neodb.BackEnd import StorageBackend


class MemoryDB(StorageBackend):
    """
    Uses python dictionary to store data
    """
    def __init__(self, name: Optional[str] = ''):
        """
        Create an instance of the MemoryDB class

        :param name: name for the database
        :return: MemoryDB instance
        """
        self.documents = {}  # store all documents/values with unique id = document_url
        self.buckets = {'/': []}  # store bucket :  [document names]
        self.name = name

    def bucket_exists(self, bucket_url: str) -> bool:
        if bucket_url in self.buckets:
            return True
        return False

    def list_buckets(self, bucket_url: str) -> Union[list, bool]:
        if bucket_url in self.buckets:
            res = []
            if bucket_url == "/":
                level = 1
            else:
                level = bucket_url.count('/') + 1
            for bucket in self.buckets.keys():
                if bucket.count('/') == level and bucket.startswith(bucket_url) and bucket != bucket_url:
                    res.append(bucket)
            return res
        return False

    def create_bucket(self, bucket_url: str) -> bool:
        if bucket_url in self.buckets:
            return False
        self.buckets[bucket_url] = []
        return True

    def delete_bucket(self, bucket_url: str) -> bool:
        if bucket_url in self.buckets:
            # first delete bucket items from documents
            for document_url in self.buckets[bucket_url]:
                self.documents.pop(document_url)
            self.buckets.pop(bucket_url)
            return True
        return False

    def document_exists(self, document_url) -> bool:
        if document_url in self.documents:
            return True
        return False

    def list_documents(self, bucket_url: str) -> Union[list, bool]:
        if bucket_url in self.buckets:
            return self.buckets[bucket_url]
        return False

    def read_document(self, document_url: str) -> Any:
        if document_url in self.documents:
            return self.documents[document_url]
        return False

    def store_document(self, document_url: str, document) -> bool:
        bucket_url, _ = document_url.rsplit('/', 1)
        if bucket_url in self.buckets:
            self.documents[document_url] = document
            self.buckets[bucket_url].append(document_url)
            return True
        return False

    def delete_document(self, document_url: str) -> bool:
        bucket_url, _ = document_url.rsplit('/', 1)
        if document_url in self.documents:
            if document_url in self.buckets[bucket_url]:
                self.buckets[bucket_url].remove(document_url)
                self.documents.pop(document_url)
                return True
        return False
