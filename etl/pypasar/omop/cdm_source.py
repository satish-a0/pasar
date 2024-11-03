import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
# Load environment variables from the .env file
from ..db.utils.postgres import postgres
load_dotenv()


class cdm_source:

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
        # Truncate always since only 1 record should exist
        with self.engine.connect() as connection:
            with connection.begin():
                # Set schema
                connection.execute(
                    text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))
                # Insert record
                connection.execute(text("DELETE FROM cdm_source"))

    def process(self):
        with self.engine.connect() as connection:
            with connection.begin():
                # Set schema
                connection.execute(
                    text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))
                # Insert record
                connection.execute(text("""INSERT INTO cdm_source (
                        cdm_source_name,
                        cdm_source_abbreviation,
                        cdm_holder,
                        source_description,
                        source_documentation_reference,
                        cdm_etl_reference,
                        source_release_date,
                        cdm_release_date,
                        cdm_version,
                        vocabulary_version,
                        cdm_version_concept_id
                    ) VALUES (
                        'PASAR',
                        'PASAR',
                        'APAC_OHDSI',
                        'PASAR',
                        'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC10834714',
                        '',
                        TO_DATE('20240809','YYYYMMDD'),
                        CURRENT_DATE,
                        'v5.4',
                        '',
                        756265
                        )"""))

    def finalize(self):
        pass
