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

        with self.engine.connect() as connection:
            with connection.begin():
                # Read from source and create a staging table (stg__note)
                # create a staging table to perform transformation and joining other required tables
                connection.execute(
                    text(f'''
                        CREATE OR REPLACE VIEW {omop_schema}.stg__note AS
                            WITH postop__clindoc AS (
                                SELECT
                                    id,
                                    anon_case_no,
                                    session_id,
                                    session_enddate,
                                    postop_clindoc_item_name,
                                    postop_clindoc_item_description,
                                    postop_clindoc_value_text,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY anon_case_no, session_id, session_enddate, postop_clindoc_item_description
                                        ORDER BY id
                                    ) AS row_num      --Any rows duplicate will show row num of more than 1
                                FROM postop.clindoc
                            ),

                            -- Filter the table to only include unique value
                            filtered AS (
                                SELECT * from postop__clindoc WHERE ROW_NUM = 1
                            ),

                            -- revert visit occurrence id to session id
                            sessionIDs AS (
                                SELECT CAST(LEFT(CAST(visit_occurrence_id AS TEXT), LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2) AS INTEGER) AS session_id, *
                                    FROM omop_sqldev_schema.visit_occurrence
                            )

                            SELECT
                                ROW_NUMBER() OVER (ORDER BY session_enddate, id) AS note_id,
                                CDM_PER.person_id AS person_id,
                                session_enddate AS note_date,
                                clindoc.postop_clindoc_item_name AS note_title,

                            -- Handle any null values in note text
                            CASE
                                WHEN clindoc.postop_clindoc_value_text IS NOT NULL 
                                THEN clindoc.postop_clindoc_value_text
                                ELSE 'NULL'
                            END AS note_text,

                                CDM_VisitOcc.visit_occurrence_id AS visit_occurrence_id,
                                clindoc.postop_clindoc_item_description AS note_source_value
                            FROM filtered AS clindoc
                            -- Join tables needed for person_id, visit_occurrence_id, visit_detail_id
                            JOIN {omop_schema}.person AS CDM_PER
                                ON clindoc.anon_case_no=CDM_PER.person_source_value
                            JOIN sessionIDs AS CDM_VisitOcc
                                ON clindoc.session_id=CDM_VisitOcc.session_id
                        '''
                     ))

                # Read from stg__note and insert into CDM table note
                connection.execute(
                    text(f'''
                        INSERT INTO {omop_schema}.note (
                            note_id,
                            person_id,
                            note_date,
                            note_datetime,
                            note_type_concept_id,
                            note_class_concept_id,
                            note_title,
                            note_text,
                            encoding_concept_id,
                            language_concept_id,
                            provider_id,
                            visit_occurrence_id,
                            visit_detail_id,
                            note_source_value,
                            note_event_id,
                            note_event_field_concept_id
                        )
                        SELECT
                            note_id,
                            person_id,
                            note_date,
                            (note_date::text || ' 00:00:00')::timestamp AS note_datetime, -- Function used to take date and join to midnight
                            32879 AS note_type_concept_id,
                            0 AS note_class_concept_id,  -- Put 0 as source_to_concept not found
                            note_title,
                            note_text, -- all value text is NULL
                            32678 AS encoding_concept_id,
                            4180186 AS language_concept_id,
                            NULL AS provider_id,
                            visit_occurrence_id,
                            NULL AS visit_detail_id, -- Set as NULL based on suggestion in GitHub
                            note_source_value,
                            NULL AS note_event_id,
                            NULL AS note_event_field_concept_id
                        FROM {omop_schema}.stg__note'''
                     ))

    def finalize(self):
        self.engine.dispose()
        # Verify if needed
        pass

