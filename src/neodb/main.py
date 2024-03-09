from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from neodb.MemoryDB import MemoryDB
from neodb.FileSystemDB import FileSystemDB


def server(backend: object = None, **kwargs: object):
    """
    main process to start server

    :param backend: memory or filesystem
    :param kwargs: base_path: folder path if backend is filesystem
    :return:
    """
    if backend == "memory":
        backend = MemoryDB()
    elif backend == 'filesystem':
        if 'base_path' in kwargs:
            backend = FileSystemDB(kwargs.get('base_path'))
    else:
        exit(1)

    app = FastAPI()

    @app.get("/")
    async def root():
        return {"message": "Mock Document DB"}

    @app.get("/buckets/{bucket_url:path}")
    async def list_buckets(bucket_url: str):
        bucket_url = "/" + bucket_url.rstrip('/')
        res = backend.list_buckets(bucket_url)
        json_compatible_item_data = jsonable_encoder(res)
        return JSONResponse(content=json_compatible_item_data, media_type="application/json")

    @app.post("/buckets/{bucket_url:path}")
    async def create_bucket(bucket_url: str):
        bucket_url = "/" + bucket_url.rstrip('/')
        if backend.create_bucket(bucket_url):
            return {'message': "bucket created:" + bucket_url}
        else:
            return {'message': "bucket cannot be created:" + bucket_url}

    @app.delete("/buckets/{bucket_url:path}")
    async def delete_bucket(bucket_url: str):
        bucket_url = "/" + bucket_url.rstrip('/')
        backend.delete_bucket(bucket_url)

    @app.get("/documents/{bucket_url:path}")
    async def list_documents(bucket_url: str) -> JSONResponse:
        """
        list documents in a bucket
        :param bucket_url:
        :return:
        """
        bucket_url = "/" + bucket_url.rstrip('/')
        res = backend.list_documents(bucket_url)
        if res:
            json_compatible_item_data = jsonable_encoder(res)
            return JSONResponse(content=json_compatible_item_data, media_type="application/json")
        else:
            raise HTTPException(status_code=404, detail="Item not found")

    @app.get("/document/{document_url:path}")
    async def get_document(document_url: str):
        """
        gets a document from bucket
        :param document_url:
        :return:
        """
        document_url = "/" + document_url.rstrip('/')
        res = backend.read_document(document_url)
        return res

    @app.post("/document/{document_url:path}")
    async def store_document(document_url: str, file: UploadFile):
        document_url = "/" + document_url.rstrip('/')
        data = await file.read()
        backend.store_document(document_url, data)

    @app.delete("/document/{document_url:path}")
    async def delete_document(document_url: str):
        document_url = "/" + document_url.rstrip('/')
        backend.delete_document(document_url)

    return app


if __name__ == "__main__":
    myapp = server(backend='memory')
else:
    # myapp = server(backend='filesystem', base_path="/tmp/mydb")
    myapp = server(backend='memory')
