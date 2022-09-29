# Python lib imports
import os
import logging
import datetime as dt
from io import StringIO

# Non-standard package imports
from graphlib import TopologicalSorter
from database.postgres import PostgresDB

# Project class imports
from utility.metadata_logger import MetadataLogger
from domain.pipeline.config_domain_pipeline import DomainPipelineConfig
from domain.pipeline.extract_load_pipeline import ExtractLoad

def log_inline_setup() -> StringIO:
    """
    Sets up logging via standard Python logging, with an IO stream as the target.
        
    Arguments:
        None
    
    Returns:
        run_log: StringIO - The log's target stream for re-use (e.g. write/flush)
    """
    run_log = StringIO()
    logging.basicConfig(stream=run_log, level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    # The logging config will remain global, but can return the StringIO buffer for now
    return run_log

def log_metadata_setup(db_target: str) -> MetadataLogger:
    """
    Sets up metadata logging class for more specific pipeline summary info to be logged to database

    Arguments:
        db_target: Desired database destination specified in environment variables: 'source' or 'target'

    Returns:
        metadata_logger: MetadataLogger - a configured database logging class
    """
    metadata_logger = MetadataLogger(db_target=db_target)

def run_domain_pipeline():
    run_log = log_inline_setup()
    metadata_logger = log_metadata_setup(db_target = "target")

    logging.info("Reading configuration")
    pipeline_config = DomainPipelineConfig("domain/config.yaml")

    metadata_logger.log(
        run_timestamp = dt.datetime.now(),
        run_status = "started",
        run_id = metadata_logger.get_new_run_id(db_table=pipeline_config.metadata_log_table),
        run_config = pipeline_config.config_raw,
        target_log_table = pipeline_config.metadata_log_table
    )

    logging.info("Setting up database configuration")
    # AJP TODO: Database setup
    db_engine_source = PostgresDB.create_pg_engine(db_target="source")
    db_engine_target = PostgresDB.create_pg_engine(db_target="target")

    logging.info("Setting up extract/load workflow")
    # AJP TODO: This is just an example ExtractLoad instantiation - TBD with city params
    for next_city in pipeline_config.cities:
        step_next_city = ExtractLoad()


if (__name__ == "__main__"):
    run_domain_pipeline()
