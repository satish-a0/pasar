import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
# Load environment variables from the .env file
load_dotenv()


class death:

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
                # Set schema
                connection.execute(
                    text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))

                # Verify that each person has a unique death_date
                result = connection.execute(
                    text(f'''SELECT (
                        case when count(distinct(anon_case_no, death_date))=count(distinct(anon_case_no)) then 'Unique' else 'Duplicate' end)
                        FROM {os.getenv("POSTOP_SOURCE_SCHEMA")}.info WHERE death_date IS NOT NULL'''
                    )).mappings().all()[0]
                if result['case'] != 'Unique':
                    raise ValueError(f'There is duplicate in death_date.')

                # Delete death table
                connection.execute(text("TRUNCATE TABLE death"))

    def process(self):
        # Set SCHEMA
        omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        source_schema = os.getenv("POSTOP_SOURCE_SCHEMA")

        with self.engine.connect() as connection:
            with connection.begin():
                # Read from source and create a staging table (stg__death)
                connection.execute(
                    text(f'''CREATE TEMP VIEW stg__death AS
                        SELECT distinct
                            cdm.person_id AS person_id,
                            cdm.person_source_value AS person_source_value,
                            source.death_date AS death_date,
                            32879 AS death_type_concept_id
                        FROM {source_schema}.info AS source
                        JOIN {omop_schema}.person AS cdm
                            ON source.anon_case_no=cdm.person_source_value
                        WHERE
                            source.death_date IS NOT NULL'''
                     ))

                # Read from stg__death and insert into death
                connection.execute(
                    text(f'''
                        INSERT INTO {omop_schema}.death (
                            person_id,
                            death_date,
                            death_datetime,
                            death_type_concept_id,
                            cause_concept_id,
                            cause_source_value,
                            cause_source_concept_id
                        )
                        SELECT
                            person_id,
                            death_date,
                            NULL AS death_datetime,
                            death_type_concept_id,
                            NULL AS cause_concept_id,
                            NULL AS cause_source_value,
                            NULL AS cause_source_concept_id
                        FROM stg__death'''
                     ))

    def finalize(self):
        self.engine.dispose()
        # Verify if needed
        pass

