# Python lib imports
import os
import logging
import itertools
import datetime as dt
from io import StringIO

# Non-standard package imports
from dotenv import load_dotenv
from graphlib import TopologicalSorter
from database.postgres import PostgresDB

# Project class imports
from utility.metadata_logger import MetadataLogger
from domain.pipeline.config_domain_pipeline import DomainPipelineConfig
from domain.pipeline.extract_load_pipeline import ExtractLoad
from domain.etl.transform import Transform

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
    return metadata_logger

def run_domain_pipeline() -> bool:
    # AJP TODO: try/catch
    load_dotenv("./domain_local.env")
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

    try:
        logging.info("Setting up database configuration")
        # Database engine only required for target database - sources are APIs
        db_engine_target = PostgresDB.create_pg_engine(db_target="target")

        logging.info("Setting up extract/load workflow")
        ts = TopologicalSorter()
        extract_load_steps = []
        extract_combinations = list(itertools.product(pipeline_config.extract_apis.keys(), pipeline_config.extract_cities))

        for next_api, next_city in extract_combinations:
            # APIs are keyed by names so just use that as the raw table names.
            # These keys are also used to pull the SQL Alchemy key columns from config.yaml
            next_table_name = f"raw_{next_api}"
            #print(next_table_name)
            step_next_city = ExtractLoad(
                base_api_url = pipeline_config.extract_apis[next_api],
                city = next_city,
                db_engine = db_engine_target,
                table_name = next_table_name,
                log_path = pipeline_config.extract_log_path,
                key_columns = pipeline_config.load_key_columns[next_api],
                mode = pipeline_config.load_mode
            )

            # Add node to workload independently, but also compile to list
            # This way the list can be unpacked as a dependency later
            extract_load_steps.append(step_next_city)
            ts.add(step_next_city)

        # AJP TODO: Example transformation pipeline
        step_transform_stub = Transform(model = "example_model", db_engine = db_engine_target, model_path = pipeline_config.transform_model_path)

        # Generic unpack with example transform step
        #ts.add(step_transform_stub, *extract_load_steps)

        logging.info("Executing completed workflow")
        workflow = tuple(ts.static_order())
        for next_step in workflow:
            #print(next_step.base_api_url, next_step.city)
            next_step.run()

        logging.info("Pipeline has finished")

        metadata_logger.log(
            run_timestamp = dt.datetime.now(),
            run_status = "completed",
            run_id = metadata_logger.get_new_run_id(db_table=pipeline_config.metadata_log_table),
            run_config = pipeline_config.config_raw,
            run_log = run_log.getvalue(),
            target_log_table = pipeline_config.metadata_log_table
        )

    except Exception as ex:
        logging.exception(ex)
        print(ex)
        metadata_logger.log(
            run_timestamp = dt.datetime.now(),
            run_status = "error",
            run_id = metadata_logger.get_new_run_id(db_table=pipeline_config.metadata_log_table),
            run_config = pipeline_config.config_raw,
            run_log = run_log.getvalue(),
            target_log_table = pipeline_config.metadata_log_table
        )        

if (__name__ == "__main__"):
    run_domain_pipeline()
