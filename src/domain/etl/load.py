# Non-standard package imports
import pandas as pd
from sqlalchemy.engine import Engine

class Load():
    
    db_engine: Engine
    target_table: str    

    def __init__(self,
        target_table: str,
        db_engine: Engine
    ):
        self.target_table = target_table
        self.db_engine = db_engine

    def load_overwrite(self,
        data: pd.DataFrame
    ):
        pass

    def load_upsert(self,
        data: pd.DataFrame
    ):
        pass