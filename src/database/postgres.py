from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os 

class PostgresDB():

    @staticmethod
    def create_pg_engine(db_target:str="source"):
        """
        Static helper function that creates an engine object for use in loading/transformation.
        This function relies solely on environment variables to grab source or target database connection details.

        Arguments:
            db_target: Acceptable values of source or target - determines which environment variables to grab

        Returns:
            engine: A database engine (sqlalchemy.engine.Engine)
        """
        db_user = os.environ.get(f"{db_target}_db_user")
        db_password = os.environ.get(f"{db_target}_db_password")
        db_server_name = os.environ.get(f"{db_target}_db_server_name")
        db_database_name = os.environ.get(f"{db_target}_db_database_name")

        # create connection to database 
        connection_url = URL.create(
            drivername = "postgresql+pg8000", 
            username = db_user,
            password = db_password,
            host = db_server_name, 
            port = 5432,
            database = db_database_name, 
        )

        engine = create_engine(connection_url)
        return engine 