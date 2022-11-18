import logging
import numpy as np
import pandas as pd
from sqlalchemy import Column, BigInteger, MetaData, Numeric, String, DateTime, Boolean, Table
from sqlalchemy.engine import Engine
from sqlalchemy.dialects import postgresql
from domain.load.loaders.loader import Loader
from domain.load.loaders.loader_file import LoaderFile

class LoaderCloudStorage(Loader):
    bucket_url: str
    file_path: str
    file_name: str
    mode: str

    def __init__(self,
        bucket_url: str,
        file_path: str,
        file_name: str,
        mode: str
    ):
        self.bucket_url = bucket_url
        self.file_path = file_path
        self.file_name = file_name
        self.mode = mode

    def load(self,
        data: pd.DataFrame
    ) -> bool:

        loader_file = LoaderFile(
            self.file_path,
            self.file_name
        )

        success = loader_file.load(data)

        return success