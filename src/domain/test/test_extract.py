# Python lib imports
import os

# Non-standard package imports
import pandas as pd
from dotenv import load_dotenv

# Project class imports
from domain.etl.extract import Extract
from domain.pipeline.config_domain_pipeline import DomainPipelineConfig

def test_extract_sales_results():
    # Assemble
    pipeline_config = DomainPipelineConfig("domain/config.yaml")

    load_dotenv("./domain_local.env")

    expected_columns = ['city', 'url', 'execution_time', 'auctionedDate',
       'lastModifiedDateTime', 'adjClearanceRate', 'median', 'numberAuctioned',
       'numberListedForAuction', 'numberSold', 'numberUnreported',
       'numberWithdrawn', 'totalSales']

    expected_columns.sort()

    api_url = pipeline_config.extract_apis["sales_results"]
    city = "Sydney"

    # Act
    extractor = Extract(
        api_url = api_url,
        city = city
    )

    test_data = extractor.extract_api()

    # Assert
    test_columns = test_data.columns.to_list()
    test_columns.sort()

    assert len(test_data.columns) == len(expected_columns)
    assert test_columns == expected_columns

    # This test should only give back results for Sydney
    assert test_data["city"].value_counts()["Sydney"] == len(test_data)


def test_extract_sales_listings():
    # Assemble
    pipeline_config = DomainPipelineConfig("domain/config.yaml")

    load_dotenv("./domain_local.env")

    expected_columns = ['city', 'url', 'execution_time', 'agencyId', 'id', 'propertyDetailsUrl',
       'agencyName', 'agencyProfilePageUrl', 'agent', 'bathrooms', 'bedrooms',
       'carspaces', 'postcode', 'propertyType', 'result', 'state',
       'streetName', 'streetNumber', 'streetType', 'suburb', 'unitNumber',
       'geoLocation.latitude', 'geoLocation.longitude', 'price']
    expected_columns.sort()    

    api_url = pipeline_config.extract_apis["sales_listings"]
    city = "Sydney"

    # Act
    extractor = Extract(
        api_url = api_url,
        city = city
    )

    test_data = extractor.extract_api()

    # Assert
    test_columns = test_data.columns.to_list()
    test_columns.sort()

    assert len(test_data.columns) == len(expected_columns)
    assert test_columns == expected_columns 

    # This test should only give back results for Sydney
    assert test_data["city"].value_counts()["Sydney"] == len(test_data)