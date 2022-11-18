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
        """
        Parameter setup for any Domain API requests.

        Arguments:
            None - all based on class fields

        Returns:
            Dictionary of parameters for Domain API calls
        """

        # At the moment, only API key is a required parameter. The rest is achieved through URL manipulation
        return_value = {
            "api_key": os.getenv("source_api_key")
        }

        return return_value

    def get_api_url(self) -> str:
        """
        URL setup for Domain API requests.

        Arguments:
            None - all based on class fields

        Returns:
            URL to call API with
        """

        # At the moment, only city filters are supported.
        return_value = self.api_url.replace("{city}", self.city)
        return return_value

    def get_api_data(self) -> dict:
        """
        API call for Domain API.

        Arguments:
            None - all based on class fields

        Returns:
            If successful, API data in JSON format.
        """
        return_value = None

        url = self.get_api_url()
        params = self.get_params()
        response = requests.get(url=url, params=params)

        if (response.status_code == 200):
            return_value = response.json()

        return return_value

    def api_response_to_dataframe(self, 
        response_data: dict
    ) -> pd.DataFrame:
        return_value = None
        
        # Potential improvements: log a null result - although the log will show no rows were extracted today...
        if (response_data is not None):
            return_value = pd.json_normalize(data=response_data)
            return_value['city'] = self.city
            return_value['url'] = self.get_api_url()
            return_value['execution_time'] = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return return_value

    def extract_api(self) -> pd.DataFrame:
        """
        Main extract method. Calls Domain API endpoints, turns result into dataframe, then adds the city/API URL and execution time to the result.

        Arguments:
            None - all based on class fields
        
        Returns:
            If successful, API data in Pandas DataFrame format.
        """
        return_value = None

        response_data = self.get_api_data()
        logging.info(f"API response: {response_data}")
        return_value = self.api_response_to_dataframe(response_data = response_data)

        return return_value