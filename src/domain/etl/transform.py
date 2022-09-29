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

    def run(self):
        pass
