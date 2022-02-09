from typing import List
import os
import pandas as pd


class JSONTransformer:
    """ Transform files from .json to .parquet """

    def __init__(self, files: List, dest_path: str) -> None:
        """Initialize JSON transformer

        Args:
            files: a list of files paths.
            dest_path: the folder to save files.
        """

        self.files = [os.path.normpath(f) for f in files]
        self.dest_path = os.path.normpath(dest_path)


    def remove(self) -> None:
        """ Remove listed files """

        for file in self.files:
            os.remove(file)


    def run(self) -> None:
        """ Run the transformation over listed files """

        for file in self.files:

            # We have to change the .json structure to use chunksize
            df = pd.read_json(file)

            filename = os.path.splitext(file)[-2]
            path = os.path.join(self.dest_path, f'{filename}.parquet')

            df.to_parquet(path)

        self.remove()

