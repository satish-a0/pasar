import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
from ..db.utils.PostgresGCP import PostgresGCP
# Load environment variables from the .env file
load_dotenv()


class observation:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        self.engine_source = PostgresGCP().get_engine()  # Get Source PG Connection

    def execute(self):
        try:
            self.initialize()
            self.process()
            self.finalize()
        except Exception as err:
            print(f"Error occurred {self.__class__.__name__}")
            raise err

    def initialize(self):
        pass

    def process(self):

        # Example query from source postgres
        with self.engine_source.connect() as connection:
            res = connection.execute(
                text("SELECT * from preop.char LIMIT 5;")).fetchall()
            print(res)

        # In batches
        # Read from source
        # Transform
        # Ingest into OMOP Table

    def finalize(self):
        # Verify if needed
        self.engine.dispose()
        self.engine_source.dispose()
