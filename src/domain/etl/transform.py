# Python lib imports
import os
import logging

# Non-standard package imports
import jinja2 as j2
from sqlalchemy.engine import Engine

class Transform():
    
    model: str
    db_engine: Engine
    model_path: str

    def __init__(self,
        model: str,
        db_engine: Engine,
        model_path: str = "models"
    ):
        self.model = model
        self.db_engine = db_engine
        self.model_path = model_path

    def run(self) -> bool:
        """
        Builds models with a matching file name in the models_path folder. 
        - `model`: the name of the model (without .sql)
        - `models_path`: the path to the models directory containing the sql files. defaults to `models/`
        """
    
        if f"{self.model}.sql" in os.listdir(self.model_path):
            logging.info(f"Building model: {self.model}")
        
            # read sql contents into a variable 
            with open(f"models/{self.model}.sql") as f: 
                raw_sql = f.read()

            # parse sql using jinja 
            parsed_sql = j2.Template(raw_sql).render(target_table = self.model, engine=self.db_engine)

            # execute parsed sql 
            self.db_engine.execute(parsed_sql)
            logging.info(f"Successfully built model: {self.model}")
            return True 
        else: 
            logging.error(f"Could not find model: {self.model}")        
