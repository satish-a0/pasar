import traceback
import os
import io
import csv
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from ..db.utils.postgres import postgres
# Load environment variables from the .env file
load_dotenv()

class concept_relationship:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.source_file = os.path.join(os.getenv("BASE_PATH"), "transformed", "CONCEPT_RELATIONSHIP.csv")

    def execute(self):
        try:
            self.initialize()
            self.process()
            self.finalize()
        except Exception as err:
            print(f"Error occurred {self.__class__.__name__}")
            raise err

    def initialize(self):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f'DELETE FROM {self.omop_schema}.concept_relationship'))

    def process(self):
        connection = self.engine.raw_connection()
        with connection.cursor() as cursor:
            try:
                cursor.copy_expert(f"COPY {self.omop_schema}.concept_relationship FROM STDIN WITH DELIMITER '\t' CSV HEADER QUOTE '\"' ESCAPE E'\\\\'" , open(self.source_file, "r", buffering=2**10))
                connection.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)

    def finalize(self):
        self.engine.dispose()