import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
# Load environment variables from the .env file
load_dotenv()


class note:

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

                # Drop stg__note view if exists
                connection.execute(text("DROP VIEW IF EXISTS stg__note"))

                # Delete note table
                connection.execute(text("TRUNCATE TABLE note"))

    def process(self):
        # Set SCHEMA
        omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        # source_schema = os.getenv("POSTGRES_SOURCE_SCHEMA")

        with self.engine.connect() as connection:
            with connection.begin():
                # Read from source and create a staging table (stg__note)
                # create a staging table to perform transformation and joining other required tables
                connection.execute(
                    text(f'''
                        CREATE OR REPLACE VIEW {omop_schema}.stg__note AS
                        SELECT DISTINCT
                            ROW_NUMBER() OVER (ORDER BY source_CliDc.postop_clindoc_created_datetime, id) AS note_id,
                            CDM_PER.person_id AS person_id,
                            CAST(source_CliDc.postop_clindoc_created_datetime AS date) AS note_date,
                            CAST(source_CliDc.postop_clindoc_created_datetime AS Time) AS note_datetime,
                            0 AS note_class_concept_id,
                            source_CliDc.postop_clindoc_item_name AS note_title,
                            source_CliDc.postop_clindoc_value_text AS note_text,
                            0 AS provider_id,
                            source_CliDc.session_id AS visit_occurrence_id,
                            0 AS visit_detail_id,
                            source_CliDc.postop_clindoc_item_description AS note_source_value,
                            0 AS note_event_id,
                            0 AS note_event_source_concept_id
                        FROM postop.clindoc AS source_CliDc
                        JOIN {omop_schema}.person AS CDM_PER
                            ON source_CliDc.anon_case_no=CDM_PER.person_source_value
                        '''
                     ))

                # Note: will join other CDM tables once populated with data
                # No inserting data into the actual CDM table as stg_note has incomplete data
                # Read from stg__note and insert into CDM table note
                # """ connection.execute(
                #     text(f'''
                #         INSERT INTO {omop_schema}.note (
                #             note_id,
                #             person_id,
                #             note_date,
                #             note_datetime,
                #             38279 AS note_type_concept_id,
                #             note_class_concept_id,
                #             note_title,
                #             note_text,
                #             32678 AS encoding_concept_id,
                #             4180186 AS language_concept_id,
                #             provider_id,
                #             visit_occurrence_id,
                #             visit_detail_id,
                #             note_source_value,
                #             note_event_id,
                #             note_event_field_concept_id
                #         )
                #         SELECT
                #             note_id,
                #             person_id,
                #             note_date,
                #             note_datetime,
                #             note_type_concept_id,
                #             note_class_concept_id,
                #             note_title,
                #             note_text,
                #             encoding_concept_id,
                #             language_concept_id,
                #             provider_id,
                #             visit_occurrence_id,
                #             visit_detail_id,
                #             note_source_value,
                #             note_event_id,
                #             note_event_field_concept_id
                #         FROM {omop_schema}.stg__note'''
                #      )) """

    def finalize(self):
        self.engine.dispose()
        # Verify if needed
        pass

