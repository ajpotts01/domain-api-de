# Python lib imports
import os
import logging

# Non-standard package imports
from sqlalchemy.engine import Engine

# Project class imports
from domain.etl.extract import Extract
from domain.etl.load import Load

class ExtractLoad():
    
    base_api_url: str
    city: str
    target_engine: Engine
    table_name: str
    log_path: str
    mode: str
    key_columns: list

    def __init__(self,
        base_api_url: str,
        city: str,
        db_engine: Engine,
        table_name: str,
        log_path: str,
        mode: str = "full",
        key_columns: list = []
    ):
        self.base_api_url = base_api_url
        self.city = city
        self.db_engine = db_engine
        self.table_name = table_name
        self.log_path = log_path
        self.mode = mode
        self.key_columns = key_columns

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
                target_table = self.table_name,
                db_engine = self.db_engine,
                mode = self.mode,
                key_columns = self.key_columns
            )

            logging.info("Loading to database...")
            loader.load(df_extract)

            success = True

        return success
