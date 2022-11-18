# Non-standard package imports
import logging
import pandas as pd

from domain.load.loaders.loader import Loader

class Load():
    
    loader: Loader
    key_columns: list

    def __init__(self,
        load_destination: Loader,
        key_columns: list
    ):
        self.loader = load_destination
        self.key_columns = key_columns # TODO: Make this a base method of loader?

    # Design decision for now - de-dupe at load time. Should this be done at the T stage? Maybe.
    def deduplicate_data(self,
        data: pd.DataFrame
    ) -> pd.DataFrame:
        deduped_data = data.drop_duplicates(subset=self.key_columns)
        return deduped_data

    def load(self,
        data: pd.DataFrame 
    ) -> bool:
        
        data = self.deduplicate_data(data)

        success = self.loader.load(data)

        return success