import os
from typing import List
import pysftp
import logging


class SFTPConnector:
    """ The SFTP connector to handle external data """

    def __init__(self, host: str, port: int, username: str, key_path: str,
            files: List) -> None:
        """Initialize the SFTP connector

        Args:
            host: the IP of the remote machine
            port: The SSH port of remote machine
            username: username at remote machine
            key: path to private key
            files: files list to download
        """

        self.files = [os.path.normpath(f) for f in files]
        self.tmp = os.path.join(os.path.dirname(__file__), '..', 'tmp')

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.connection = pysftp.Connection(
            host=host, port=port, username=username,
            private_key=key_path, cnopts=cnopts
        )


    def download(self, file: str) -> None:
        """ Copy a file between the remote host and the local host

        Args:
            source_path: the source path
        """
        path = os.path.join(self.tmp, file)
        self.connection.get(file, path)


    def list_dir(self, path: str = '.') -> List:
        """ List files and directories given path

        Args:
            path: the path to list

        Return:
            A list of files and directories
        """

        return ([f.filename for f in self.connection.listdir_attr(path)])


    def close(self) -> None:
        """ Close the connection and cleans up """

        self.connection.close()


    def run(self) -> None:
        """ Download files if available """

        available_files = self.list_dir()

        for file in self.files:
            if file in available_files:

                logging.info(f'Downloading {file}')
                self.download(file)
