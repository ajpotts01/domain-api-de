from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os 
import ssl

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
            database = db_database_name
        )

        require_ssl = False
        ssl_path = ''

        if ("require_ssl" in os.environ.keys() and "ssl_cert" in os.environ.keys()):
            require_ssl = os.environ.get("require_ssl")
            ssl_path = os.environ.get("ssl_cert")

            if (require_ssl):
                print(f"Setting SSL context - require_ssl: {require_ssl}, path: {ssl_path}")
                ssl_context = ssl.create_default_context()
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                ssl_context.load_verify_locations(ssl_path)
            engine = create_engine(connection_url, connect_args = {'ssl_context': ssl_context})
        else:
            engine = create_engine(connection_url)

        return engine 