import os
from typing import List
from hdfs import InsecureClient


class HDFSLoader:
    """ The HDFS data loader """

    def __init__(self, host: str, port: int, username: str, files: List) -> None:
        """Initialize the HDFS loader

        Args:
            host: the IP of the hdfs
            port: The SSH port of hdfs
            username: hdfs folder to save files
            files: files list to load
        """

        self.username = username
        self.files = [os.path.normpath(f) for f in files]
        self.tmp = os.path.join(os.path.dirname(__file__), '..', 'tmp')

        self.hdfs = InsecureClient(f'http://{host}:{port}', user=username)


    def run(self):

        available_files = os.listdir(self.tmp)

        for file in self.files:
            if file in available_files:

                self.hdfs.upload(
                    os.path.join('user', self.username, file),
                    os.path.join(self.tmp, file)
                )


