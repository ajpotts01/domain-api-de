import logging
import numpy as np
import pandas as pd
from sqlalchemy import Column, BigInteger, MetaData, Numeric, String, DateTime, Boolean, Table
from sqlalchemy.engine import Engine
from sqlalchemy.dialects import postgresql
from domain.load.loaders.loader import Loader

class LoaderDatabase(Loader):
    db_engine: Engine
    target_table: str    
    mode: str
    key_columns: list
    chunksize: int

    def __init__(self,
        target_table: str,
        db_engine: Engine,
        mode: str = "full",
        key_columns: list = [],
        chunksize: int = 1000
    ):
        self.target_table = target_table
        self.db_engine = db_engine
        self.mode = mode
        self.key_columns = key_columns
        self.chunksize = chunksize    


    def load(self,
        data: pd.DataFrame 
    ) -> bool:
        
        if (self.mode == "upsert"):
            logging.info(f"Performing conventional upsert into table {self.target_table}")
            success = self.load_upsert(data)
        elif (self.mode == "upsert_chunks"):
            logging.info(f"Performing upsert in chunks into table {self.target_table}")
            success = self.load_upsert_chunks(data)
        elif (self.mode == "incremental"):
            logging.info(f"Performing incremental load into table {self.target_table}")
            success = self.load_incremental(data)
        else:
            logging.info(f"Performing complete overwrite of table {self.target_table}")
            success = self.load_overwrite(data)

        return success

    def load_overwrite(self,
        data: pd.DataFrame
    ) -> bool:
        logging.info(f"Writing {len(data)} rows to table: {self.target_table}")

        data.to_sql(name=self.target_table, con=self.db_engine, if_exists="replace", index=False)
        logging.info("Load successful")
        
        success = True

        return success

    def load_upsert(self,
        data: pd.DataFrame
    ) -> bool:
        """
        Database upsert function for loading Domain API results.

        Arguments:
            data: Pandas dataframe containing the desired data to load after extract from Domain API

        Returns:
            True if successful (nothing if it fails)
        """    
        table_meta = MetaData()

        logging.info(f"Generating table schema for {self.target_table}")
        logging.info(f"Key columns are: {self.key_columns}")

        table_schema = self.generate_sqlalchemy_schema(df = data, meta = table_meta)
        table_meta.create_all(self.db_engine)

        logging.info(f"Generated table schema. Now writing data")

        # Replace any NaN values with db-friendly NULLs.
        # NOTE: This HAS to be done after schema is generated, but before the load.
        # pd.DataFrame.replace({np.nan: None}) actually changes ALL affected columns to object!
        # So you'll end up with incorrect data type mismatches etc.
        db_friendly_data = data.replace({np.nan: None})        
        # AJP TODO: Support chunksize. Maybe turn this into a function on its own.
        insert_stmt = postgresql.insert(table_schema).values(db_friendly_data.to_dict(orient='records'))
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements = self.key_columns,
            set_ = {col.key: col for col in insert_stmt.excluded if col.key not in self.key_columns}
        )

        result = self.db_engine.execute(upsert_stmt)
        logging.info(f"A total of {result.rowcount} rows were added or modified.")
        return True

    def load_upsert_chunks(self, data:pd.DataFrame)->bool:
        """
        Database upsert function for loading large sets of Domain API results in chunks.
        The functionality is largely the same as load_upsert, just breaking things up into chunks.
        Assume Load.chunksize is set.

        Arguments:
            data: Pandas dataframe containing the desired data to load after extract from Domain API.

        Returns:
            True if successful
        """
        logging.info(f"Generating table schema for {self.target_table}")
        logging.info(f"Key columns are: {self.key_columns}")
        table_meta = MetaData()
        table_schema = self.generate_sqlalchemy_schema(df = data, meta = table_meta)
        table_meta.create_all(self.db_engine)

        logging.info(f"Generated table schema. Now writing data")

        max_length = len(data)
        
        # This needs to be performed for type safety (Postgres won't allow np.nan - treats as string)
        # But needs to be done after schema generation - see load_upsert for more details
        data = data.replace({np.nan: None})
        for i in range(0, max_length, self.chunksize):
            if i + self.chunksize >= max_length: 
                lower_bound = i
                upper_bound = max_length 
            else: 
                lower_bound = i 
                upper_bound = i + self.chunksize
            insert_statement = postgresql.insert(table_schema).values(data.iloc[lower_bound:upper_bound].to_dict(orient='records'))
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=self.key_columns,
                set_={c.key: c for c in insert_statement.excluded if c.key not in self.key_columns})
            logging.info(f"Inserting chunk: [{lower_bound}:{upper_bound}] out of index {max_length}")
            result = self.db_engine.execute(upsert_statement)
        return True 

    def load_incremental(self,
        data: pd.DataFrame
    ) -> bool:
        """
        Helper function for pure incremental loads. Does nothing yet - haven't established if this is going to be done for this project
        Or if Domain API will even function in a way that allows it.

        Coming back to this after cloud infrastructure implementation and documentation etc. if there is time.
        """    
        raise NotImplementedError

    def get_sqlalchemy_column(self,
        col_name: str,
        source_dtype: str,
        primary_key: bool = False
    ) -> Column:
        """
        Helper function that maps Pandas columns to SQL Alchemy columns

        Arguments:
            col_name: Column name from Pandas (to keep things consistent)
            source_dtype: Source Pandas data type e.g. float64, int64
            primary_key: Flag to indicate if this needs to be a primary key column

        Returns:
            mapped_column: Column - a SQLAlchemy column type
        """
        dtype_map = {
            "int64": BigInteger, 
            "object": String, 
            "datetime64[ns]": DateTime, 
            "float64": Numeric,
            "bool": Boolean
        }
        
        logging.info(f"Mapping {col_name} - {source_dtype} to {dtype_map[source_dtype]}")
        mapped_column = Column(col_name, dtype_map[source_dtype], primary_key=primary_key) 
        return mapped_column

    def generate_sqlalchemy_schema(self,
        df: pd.DataFrame, 
        meta: MetaData
    ) -> Table:
        """
        Helper function that generates the correct schema for a table in SQLAlchemy

        Arguments:
            df: Pandas dataframe containing the data to load
            meta: MetaData object for SQLAlchemy to use

        Returns:
            mapped_table: Table - a SQLAlchemy table type
        """

        schema = []
        # Use list comprehension + zip to create a list of dictionaries
        # This dictionary functions as parameters to unpack into the get_sqlalchemy_column method.
        # AJP TODO: See if there's a less roundabout way to do this (but there probably isn't)
        columns = zip(df.columns, [dtype.name for dtype in df.dtypes])
        cols_dtypes = [{"col_name": col[0], "source_dtype": col[1]} for col in columns]
        logging.info(f"Data types: {cols_dtypes}")
        for next_pair in cols_dtypes:
            # Note: Using **kwargs at the start is the same as trying to specify named params
            # e.g. you cannot just randomly add positional arguments afterwards.
            mapped_column = self.get_sqlalchemy_column(**next_pair, primary_key = next_pair["col_name"] in self.key_columns)
            schema.append(mapped_column)
        
        # Then unpack the schema to create table!
        return Table(self.target_table, meta, *schema)