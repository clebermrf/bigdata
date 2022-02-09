from connectors import SFTPConnector
from transformers import JSONTransformer


sftp = SFTPConnector(
    host='localhost',
    port=2222,
    username='vendor',
    key_path='id_rsa'
)

transformer = JSONTransformer(
    files=['etl/authors.json', 'etl/books.json', 'etl/reviews.json'],
    dest_path='vendor'
)

if __name__ == '__main__':
    transformer.run()
