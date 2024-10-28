import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres

# Load environment variables from the .env file
load_dotenv()

class care_site:

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
                # Drop the view if it exists
                connection.execute(text("DROP VIEW IF EXISTS stg__care_site"))
                # Clear all existing rows from the care_site table
                connection.execute(text("TRUNCATE TABLE care_site"))

    def process(self):
        # List of SQL file paths
        sql_files = [
            os.path.join(os.getenv("BASE_PATH"), "care_site", "stg__care_site.sql"),
            os.path.join(os.getenv("BASE_PATH"), "care_site", "care_site.sql")
        ]
        self.execute_sql_files(sql_files)

    def execute_sql_files(self, file_paths):
        # Define placeholder to environment variable mappings
        placeholder_mapping = {
            "{OMOP_SCHEMA}": os.getenv("POSTGRES_OMOP_SCHEMA"),
            "{PREOP_SCHEMA}": os.getenv("POSTGRES_SOURCE_PREOP_SCHEMA"),
            "{INTRAOP_SCHEMA}": os.getenv("POSTGRES_SOURCE_INTRAOP_SCHEMA"),
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
