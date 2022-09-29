# Python lib imports
import datetime as dt

# Non-standard package imports
from sqlalchemy import Table, Column, Integer, String, MetaData, JSON
from sqlalchemy import insert, select, func
from sqlalchemy.engine import Engine

# Project class imports
from database.postgres import PostgresDB

class MetadataLogger():

    db_engine: Engine

    def __init__(self,
        db_target: str
    ):
        self.db_engine = PostgresDB.create_pg_engine(db_target=db_target)

    def create_log_table(self,
        db_table: str
    )->Table:
        log_model = MetaData()
        log_table = Table(
            db_table,
            log_model,
            Column("run_timestamp", String, primary_key=True),
            Column("run_id", Integer, primary_key=True),
            Column("run_status", String, primary_key=True),
            Column("run_config", JSON),
            Column("run_log", String)
        )

        log_model.create_all(self.db_engine)
        return log_table

    def get_new_run_id(self,
        db_table: str
    )->int:
        log_table = self.create_log_table(db_table) # Will either return existing, or reference to new
        stmt = (select(func.max(log_table.c.run_id)))

        response = self.db_engine.execute(stmt).first()[0]
        if (response is None):
            return 1
        else:
            return response + 1

    def log(self,
        run_timestamp: dt.datetime,
        run_id: int,
        run_config: dict,
        target_log_table: str,
        run_status: str = "started",
        run_log: str = ""
    )->bool:
        log_table = self.create_log_table(db_table = target_log_table)

        # AJP TODO: If there's time - make this more robust by checking for log_table actually being a Table ref...
        stmt_insert = insert(log_table).values(
            run_timestamp = run_timestamp,
            run_id = run_id,
            run_status = run_status,
            run_config = run_config,
            run_log = run_log
        )

        self.db_engine.execute(stmt_insert)

        return True # AJP TODO: if there's time - add try/catch/return false on exception