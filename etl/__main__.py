import os
from connectors import SFTPConnector
from transformers import JSONTransformer
from loaders import HDFSLoader


files = ['authors.json', 'books.json', 'reviews.json']

sftp = SFTPConnector(
    host='localhost',
    port=2222,
    username='vendor',
    key_path='etl\\id_rsa',
    files=files
)

transformer = JSONTransformer(files=files)

files = ['authors.parquet', 'books.parquet', 'reviews.parquet']

hdfs = HDFSLoader(
    host='localhost',
    port=9870,
    username='vendor',
    files=files
)

if __name__ == '__main__':

    sftp.run()
    transformer.run()
    hdfs.run()
