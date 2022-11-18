import logging
import numpy as np
import pandas as pd
import datetime as dt

from domain.load.loaders.loader import Loader

class LoaderFile(Loader):

    file_path: str
    file_name: str
    file_format: str # csv, parquet, json

    def __init__(self,
        file_path: str,
        file_name: str,
        file_format: str = "parquet"
    ):
        self.file_path = file_path
        self.file_name = file_name
        self.file_format = file_format

    def load(self,
        data: pd.DataFrame
    ) -> bool:
        now = dt.datetime.now()
        now_string = now.strftime(now, "%Y_%m_%d_%H_%M_%S")

        full_path = f"{self.file_path}/{self.file_name}_{now_string}"

        if self.file_format == "csv":
            data.to_csv(f"{full_path}.csv")
        elif self.file_format == "parquet":
            data.to_parquet(f"{full_path}.parquet")
        elif self.file_format == "json":
            data.to_json(f"{full_path}.json")

        return True


