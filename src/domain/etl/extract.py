import pandas as pd

class Extract():
    
    api_url: str
    city: str

    def __init__(self,
        api_url: str,
        city: str
    ):
        self.api_url = api_url
        self.city = city
    
    def extract_api(self) -> pd.DataFrame:
        # Just return blank for now - fill this in with group's API code
        return pd.DataFrame()