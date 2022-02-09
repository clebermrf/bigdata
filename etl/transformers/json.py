from typing import List
import os
import pandas as pd


class JSONTransformer:
    """ Transform files from .json to .parquet """

    def __init__(self, files: List) -> None:
        """Initialize JSON transformer

        Args:
            files: a list of files paths.
        """

        self.files = [os.path.normpath(f) for f in files]
        self.tmp = [os.path.dirname(__file__), '..', 'tmp']


    def run(self) -> None:
        """ Run the transformation over listed files """

        available_files = os.listdir(os.path.join(*self.tmp))

        for file in self.files:
            if file in available_files:

                file_path = os.path.join(*self.tmp, file)
                df = pd.read_json(file_path)

                filename = os.path.splitext(file)[-2]
                df.to_parquet(os.path.join(*self.tmp, f'{filename}.parquet'))

                os.remove(file_path)

