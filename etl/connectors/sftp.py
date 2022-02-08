from typing import List
import pysftp


class SFTP:
    """ The SFTP connector to handle external data """

    def __init__(self, host:str, port:int, username:str, key_path:str) -> None:
        """Initialize the SFTP connector

        Args:
            host: the IP of the remote machine.
            port: The SSH port of remote machine.
            username: username at remote machine.
            key: path to private key
        """

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.connection = pysftp.Connection(
            host=host, port=port, username=username,
            private_key=key_path, cnopts=cnopts
        )


    def download(self, source_path:str, dest_path:str) -> None:
        """ Copy a file between the remote host and the local host

        Args:
            source_path: the source path
            dest_path: the destination path
        """
        self.connection.get(source_path, dest_path)


    def list_dir(self, path:str = '.') -> List:
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


sftp = SFTP(
    host='localhost',
    port=2222,
    username='vendor',
    key_path='id_rsa'
)

print(sftp.list_dir())
sftp.close()

