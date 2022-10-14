# Python lib imports
import os
import json

# Non-standard package imports
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.engine import Engine

# Project class imports
from domain.etl.extract import Extract
from domain.etl.load import Load
from domain.pipeline.config_domain_pipeline import DomainPipelineConfig
from database.postgres import PostgresDB

def init_env():
    load_dotenv("../domain_local.env")

def init_config():
    pipeline_config = DomainPipelineConfig("domain/config.yaml")

    return pipeline_config

def init_engine() -> Engine:
    db_engine = PostgresDB.create_pg_engine(db_target = "target")

    return db_engine

def get_mock_sales_results() -> dict:
    return_value = dict(
        auctionedDate = '2022-10-08',
        lastModifiedDateTime = '2022-10-14T05:59:21.937Z', 
        adjClearanceRate = 0.6156941649899397, 
        median = 1408000, 
        numberAuctioned = 497, 
        numberListedForAuction = 633, 
        numberSold = 306, 
        numberUnreported = 3, 
        numberWithdrawn = 100, 
        totalSales = 328073249
    )

    return return_value

def get_mock_sales_listings() -> list:
    mock_listing_a = dict(
        agencyId = 30694, 
        id = 2018022049, 
        propertyDetailsUrl = 'https=//www.domain.com.au/7-174-hampden-road-abbotsford-nsw-2046-2018022049', 
        agencyName = 'Stone Real Estate Five Dock', 
        agencyProfilePageUrl = 'https=//www.domain.com.au/real-estate-agencies/stonerealestatefivedock-30694', 
        agent = 'Stone Real Estate Five Dock', 
        bathrooms = 1, 
        bedrooms = 2, 
        carspaces = 2, 
        geoLocation = dict(latitude = -33.8549299, longitude = 151.1321622), 
        postcode = '2046', 
        propertyType = 'Unit', 
        result = 'AUPN', 
        state = 'Nsw', 
        streetName = 'Hampden', 
        streetNumber = '174', 
        streetType = 'Rd', 
        suburb = 'Abbotsford', 
        unitNumber = '7'
    )

    mock_listing_b = dict(
        agencyId = 3457, 
        id = 2018068239, 
        propertyDetailsUrl = 'https=//www.domain.com.au/8-2-6-rokeby-road-abbotsford-nsw-2046-2018068239', 
        agencyName = 'Strathfield Partners', 
        agencyProfilePageUrl = 'https=//www.domain.com.au/real-estate-agencies/strathfieldpartners-3457', 
        agent = 'Strathfield Partners Projects', 
        bathrooms = 1, 
        bedrooms = 2, 
        carspaces = 2, 
        geoLocation = dict(latitude = -33.8470749, longitude = 151.1303917), 
        postcode = '2046', 
        price = 920000, 
        propertyType = 'Unit', 
        result = 'AUSD', 
        state = 'Nsw', 
        streetName = 'Rokeby', 
        streetNumber = '2-6', 
        streetType = 'Rd', 
        suburb = 'Abbotsford', 
        unitNumber = '8'
    )

    return_value = [mock_listing_a, mock_listing_b]

    return return_value

def test_load_results():
    table_name = "mock_raw_sales_results"

    init_env()
    db_engine = init_engine()
    pipeline_config = init_config()

    test_extractor = Extract("https://domain.api.com.au", "Test City")
    test_loader = Load(target_table = table_name, db_engine = db_engine, mode = "upsert_chunks", key_columns=pipeline_config.load_key_columns["sales_results"])

    test_response = get_mock_sales_results()
    df_test_results = test_extractor.api_response_to_dataframe(test_response)
    test_loader.load(df_test_results)

    df_test_load_results = pd.read_sql(sql=table_name, con=db_engine)

    db_engine.execute(f"DROP TABLE {table_name}")

    pd.testing.assert_frame_equal(left=df_test_results, right=df_test_load_results, check_exact=True)


def test_load_listings():
    table_name = "mock_raw_sales_listings"

    init_env()
    db_engine = init_engine()
    pipeline_config = init_config()

    test_extractor = Extract("https://domain.api.com.au", "Test City")
    test_loader = Load(target_table = table_name, db_engine = db_engine, mode = "upsert_chunks", key_columns=pipeline_config.load_key_columns["sales_listings"])

    test_response = get_mock_sales_listings()
    df_test_results = test_extractor.api_response_to_dataframe(test_response)
    test_loader.load(df_test_results)

    df_test_load_results = pd.read_sql(sql=table_name, con=db_engine)

    db_engine.execute(f"DROP TABLE {table_name}")

    pd.testing.assert_frame_equal(left=df_test_results, right=df_test_load_results, check_exact=True)