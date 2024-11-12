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


class concept:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.source_file = os.path.join(os.getenv("BASE_PATH"), "vocab", "CONCEPT.csv")

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
                connection.execute(text(f'DELETE FROM {self.omop_schema}.concept'))

    def process(self):

        # Ingest into CONCEPT Table in batches
        with pd.read_csv(self.source_file, header=0, sep='\t', encoding='utf-8', quotechar='"', chunksize=int(os.getenv("PROCESSING_BATCH_SIZE"))) as reader:
            for chunk in reader:
                self.ingest(chunk)

        # Post process to set invalid_reason as null
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f"UPDATE {self.omop_schema}.concept set invalid_reason = null where invalid_reason = ''"))


    def ingest(self, df):
        buffer = io.StringIO()
        df.to_csv(buffer, sep='\t', encoding='utf-8', quotechar='"', quoting=csv.QUOTE_ALL, index=False, header=True)
        buffer.seek(0)
        # print(df.head(1))
        connection = self.engine.raw_connection()
        with connection.cursor() as cursor:
            try:
                cursor.copy_expert(f"COPY {self.omop_schema}.CONCEPT FROM STDIN WITH DELIMITER E'\t' CSV HEADER QUOTE '\"' ESCAPE E'\\\\'" , buffer)
                connection.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)

    def finalize(self):
        self.engine.dispose()
