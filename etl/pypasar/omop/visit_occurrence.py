import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres

# Load environment variables from the .env file
load_dotenv()

class visit_occurrence:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection

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
                # Set the schema for subsequent SQL operations
                connection.execute(
                    text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))
                # Drop view along with its dependent objects
                connection.execute(text("DROP VIEW IF EXISTS stg__visit_occurrence CASCADE"))
                connection.execute(text("DROP VIEW IF EXISTS int__visit_occurrence CASCADE"))
                # Clear all existing rows from the visit_occurrence table
                connection.execute(text("DELETE FROM visit_occurrence"))

    def process(self):
        # List of SQL file paths
        sql_files = [
            os.path.join(os.getenv("BASE_PATH"), "visit_occurrence", "stg__visit_occurrence.sql"),
            os.path.join(os.getenv("BASE_PATH"), "visit_occurrence", "int__visit_occurrence.sql"),
            os.path.join(os.getenv("BASE_PATH"), "visit_occurrence", "visit_occurrence.sql")
        ]
        self.execute_sql_files(sql_files)

    def execute_sql_files(self, file_paths):
        # Define placeholder to environment variable mappings
        placeholder_mapping = {
            "{OMOP_SCHEMA}": os.getenv("POSTGRES_OMOP_SCHEMA"),
            "{PREOP_SCHEMA}": os.getenv("POSTGRES_SOURCE_PREOP_SCHEMA"),
            "{POSTOP_SCHEMA}": os.getenv("POSTGRES_SOURCE_POSTOP_SCHEMA")
        }
        
        with self.engine.connect() as connection:
            with connection.begin():
                for file_path in file_paths:
                    with open(file_path, 'r') as file:
                        # Read the SQL script from the file
                        sql_script = file.read()
                        # Replace placeholders with actual values
                        for placeholder, value in placeholder_mapping.items():
                            if value is not None:
                                sql_script = sql_script.replace(placeholder, value)
                            else:
                                raise ValueError(f"Environment variable for {placeholder} not set.")
                        # Execute the SQL script
                        connection.execute(text(sql_script))

    def finalize(self):
        self.engine.dispose()