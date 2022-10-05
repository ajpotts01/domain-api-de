# Python lib imports
import os
import logging
import requests
import datetime as dt

# Non-standard package imports
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
    
    def get_params(self) -> dict:
        return_value = {
            "api_key": os.getenv("source_api_key")
        }

        return return_value

    def get_api_url(self) -> str:
        return_value = self.api_url.replace("{city}", self.city)
        return return_value

    def get_api_data(self) -> dict:
        return_value = None

        url = self.get_api_url()
        params = self.get_params()
        response = requests.get(url=url, params=params)

        if (response.status_code == 200):
            return_value = response.json()

        return return_value

    def extract_api(self) -> pd.DataFrame:
        return_value = None

        response_data = self.get_api_data()
        logging.info(f"API response: {response_data}")
        if (response_data is not None):
            return_value = pd.json_normalize(data=response_data)
            return_value['city'] = self.city
            return_value['url'] = self.get_api_url()
            return_value['execution_time'] = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return return_value