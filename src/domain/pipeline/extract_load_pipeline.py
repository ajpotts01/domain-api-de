# Python lib imports
import os
import logging

# Non-standard package imports
from sqlalchemy.engine import Engine

# Project class imports
from domain.extract.extract import Extract
from domain.load.load import Load
from domain.load.loaders.loader import Loader
from domain.load.loaders.loader_database import LoaderDatabase
from domain.load.loaders.loader_file import LoaderFile
from domain.load.loaders.loader_cloud_storage_azure import LoaderCloudStorageAzure

class ExtractLoad():
    base_api_url: str
    city: str
    db_engine: Engine
    table_name: str
    log_path: str
    load_mode: str
    load_method: str
    key_columns: list

    def __init__(self,
        base_api_url: str,
        city: str,
        db_engine: Engine,
        table_name: str,
        log_path: str,
        load_mode: str = "full",
        load_method: str = "file",
        key_columns: list = []
    ):
        self.base_api_url = base_api_url
        self.city = city
        self.db_engine = db_engine
        self.table_name = table_name
        self.log_path = log_path
        self.load_mode = load_mode
        self.load_method = load_method
        self.key_columns = key_columns

    def get_loader(self) -> Loader:
        if (self.load_method == "file"):
            return LoaderFile("test", "test")
        elif (self.load_method == "database"):
            return LoaderDatabase(
                target_table = self.table_name,
                db_engine = self.db_engine,
                mode = self.load_mode,
                key_columns = self.key_columns
            )
        elif (self.load_method == "cloud_storage"):
            if (self.load_mode == "azure"):
                return LoaderCloudStorageAzure(
                    os.getenv("bucket_url"),
                    "domain_api_file",
                    "domain_api_extract.parquet"
                )
        
        return None

    def run(self) -> bool:
        """
        Main workflow for extract/load process.

        Creates one extractor and one loader for the city and URL specified, then runs both in sequence.
        Will log rows extracted from the Domain API.

        Arguments:
            None - all based on class fields

        Returns:
            success: Flag to indicate success/failure - will report failure in case of exceptions
        """

        extractor = Extract(
            api_url = self.base_api_url,
            city = self.city
        )

        logging.info(f"Running API extract - {self.base_api_url} - {self.city}...")
        df_extract = extractor.extract_api()

        if (df_extract is None):
            logging.info("No data was extracted today.")
            success = False
        else:
            logging.info(f"{len(df_extract)} rows extracted today.")

            loader = Load(
                self.get_loader(),
                self.key_columns
            )

            logging.info("Loading now...")
            loader.load(df_extract)

            success = True

        return success
