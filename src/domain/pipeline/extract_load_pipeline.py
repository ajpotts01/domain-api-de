# Python lib imports
import os

# Non-standard package imports
from sqlalchemy.engine import Engine

# Project class imports
from domain.etl.extract import Extract
from domain.etl.load import Load

class ExtractLoad():
    
    base_api_url: str
    cities: list
    target_engine: Engine
    table_name: str
    log_path: str
    mode: str

    def __init__(self,
        base_api_url: str,
        cities: list,
        db_engine: Engine,
        table_name: str,
        log_path: str,
        mode: str = "full"
    ):
        self.base_api_url = base_api_url
        self.cities = cities
        self.db_engine = db_engine
        self.table_name = table_name
        self.log_path = log_path
        self.mode = mode


    def run(self) -> bool:
        # AJP TODO: Review design decision with the group:
        # Should an extractor take all the cities + one base api URL and concat the results etc?
        # Or should there be one Extract/Load per endpoint + city combo, and this just upserts to same table based on city name?
        # Review with the guys.
        extractor = Extract(
            api_url = self.base_api_url,
            city = "Sydney"
        )

        df_extract = extractor.extract_api()

        loader = Load(
            target_table = self.table_name,
            db_engine = self.db_engine
        )

        if (self.mode == "incremental"):
            loader.load_upsert(df_extract)
        else:
            loader.load_overwrite(df_extract)

        return True
