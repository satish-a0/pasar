import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres

# Load environment variables from the .env file
load_dotenv()


class drug_exposure:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        self.drugdrug_view = "temp_drugdrug_view"
        self.drugmed_view = "temp_drugmed_view"
        self.drug_exposure_stg_view = "stg__drug_exposure"
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.intraop_schema = os.getenv("POSTGRES_SOURCE_INTRAOP_SCHEMA")
        self.drug_exposure_table = "drug_exposure"
        #self.postop_schema = os.getenv("POSTGRES_SOURCE_POSTOP_SCHEMA")

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
                    text(f'SET search_path TO {self.omop_schema}')
                )
                
                # Drop views created
                connection.execute(text(f"DROP VIEW IF EXISTS {self.drug_exposure_stg_view} CASCADE"))
                connection.execute(text(f"DROP VIEW IF EXISTS {self.drugdrug_view}"))
                connection.execute(text(f"DROP VIEW IF EXISTS {self.drugmed_view}"))
                
                # Clear all existing rows from the drug_exposure table
                connection.execute(text(f"TRUNCATE TABLE {self.drug_exposure_table}"))

    def process(self):
        # In batches
        # Read from source
        # Transform
        # Ingest into OMOP Table
        with self.engine.connect() as connection:
            with connection.begin():
                # List of SQL file paths
                sql_files = [
                    os.path.join(os.getenv("BASE_PATH"), f"{self.drug_exposure_table}/{self.drugdrug_view}.sql"),
                    os.path.join(os.getenv("BASE_PATH"), f"{self.drug_exposure_table}/{self.drugmed_view}.sql"),
                    os.path.join(os.getenv("BASE_PATH"), f"{self.drug_exposure_table}/{self.drug_exposure_stg_view}.sql"),
                    os.path.join(os.getenv("BASE_PATH"), f"{self.drug_exposure_table}/{self.drug_exposure_table}.sql")
                ]
                self.execute_sql_files(sql_files)

    def execute_sql_files(self, file_paths):
        # Define placeholder to environment variable mappings
        placeholder_mapping = {
            "{OMOP_SCHEMA}": self.omop_schema,
            "{INTRAOP_SCHEMA}": self.intraop_schema,
            "{DRUGMED_STCM_VIEW}": self.drugmed_view,
            "{DRUGDRUG_STCM_VIEW}": self.drugdrug_view,
            "{DRUG_EXPOSURE_STG_VIEW}": self.drug_exposure_stg_view,
            "{DRUG_EXPOSURE_TABLE}": self.drug_exposure_table
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
        # Verify if needed
        self.engine.dispose()
