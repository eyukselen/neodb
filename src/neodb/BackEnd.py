"""
This module has abstract base class. Not intended for use by import
"""

from abc import ABC, abstractmethod
from typing import Any, Union, List


class StorageBackend(ABC):
    """
    Abstract base class for all operations on storage.

    For all storage mechanisms, this class provides abstract methods for
    creating, reading, listing and deleting buckets and documents
    """

    @abstractmethod
    def bucket_exists(self, bucket_url: str) -> bool:
        """
        returns True or false depending on given bucket exists

        :param bucket_url: url of the bucket to check
        :return: bool: True | False
        """

    @abstractmethod
    def list_buckets(self, bucket_url: str) -> Union[List, bool]:
        """
        returns a list of buckets under given bucket.
        :param bucket_url: url of the bucket
        :return: list: list of bucket urls
        """

    @abstractmethod
    def create_bucket(self, bucket_url: str) -> bool:
        """
        creates a new bucket
        :param bucket_url: url of the bucket
        :return:
        """

    @abstractmethod
    def delete_bucket(self, bucket_url: str) -> bool:
        """
        deletes given bucket with all of its contents
        :param bucket_url: url of the bucket
        :return:
        """

    @abstractmethod
    def document_exists(self, document_url) -> bool:
        """
        returns True or false depending on given document exists
        :param document_url: url of the bucket
        :return: bool: True | False
        """

    @abstractmethod
    def list_documents(self, bucket_url: str) -> Union[List, bool]:
        """
        returns a list of documents under given bucket
        :param bucket_url: url of the bucket
        :return: list: list of document urls or false if bucket not exist
        """

    @abstractmethod
    def read_document(self, document_url) -> Any:
        """
        Returns a document for given url
        :return: returns the document for given document_url or false if document not exist
        """

    @abstractmethod
    def store_document(self, document_url: str, document: Any) -> bool:
        """
        stores a document in backend if it exists overwrite
        :param document_url: url of the document to be stores
        :param document: document data to be stored
        :return: True if successful
        """

    @abstractmethod
    def delete_document(self, document_url: str) -> bool:
        """
        deletes a document from backend
        :param document_url:
        :return: True if successful
        """
