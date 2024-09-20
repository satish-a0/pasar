import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
# Load environment variables from the .env file
load_dotenv()


class device_exposure:

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

                # Drop stg__device_exposure view if exists
                connection.execute(text("DROP VIEW IF EXISTS stg__device_exposure"))

                # Delete device_exposure table
                connection.execute(text("TRUNCATE TABLE device_exposure"))

    def process(self):
        # In batches
        omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        source_schema = os.getenv("POSTGRES_SOURCE_SCHEMA")
        omop_sqldev_schema = "omop_sqldev_schema"

        # Read from source
        with self.engine.connect() as connection:
            with connection.begin():
        # Transform
                connection.execute(
                    text(f'''
                        CREATE OR REPLACE VIEW {omop_schema}.stg__device_exposure AS
                        SELECT distinct
                            ROW_NUMBER() OVER (ORDER BY source.id, endotracheal_tube_insertion_date, 
                            tracheostomy_tube_insertion_date) 
                            AS device_exposure_id, -- Autogenerate number
                         
                            cdm.person_id AS person_id,
                            cdm.person_source_value AS person_source_value,
                         
                            -- Handle device_concept_id
                            CASE
                                WHEN source.endotracheal_tube_insertion_date IS NOT NULL 
                                THEN 4097216
                                WHEN source.tracheostomy_tube_insertion_date IS NOT NULL
                                THEN 2616666
                                ELSE 0
                            END AS device_concept_id,
                         
                            -- Handle device_exposure_start_date
                            COALESCE(source.endotracheal_tube_insertion_date, source.tracheostomy_tube_insertion_date) 
                            AS device_exposure_start_date,
                         
                            -- Handle device_exposure_end_date
                            COALESCE(source.endotracheal_tube_removal_date, source.tracheostomy_tube_removal_date) 
                            AS device_exposure_end_date,
                         
                            -- Handle device_source_value based on which insertion date is used
                            CASE
                                WHEN source.endotracheal_tube_insertion_date IS NOT NULL 
                                THEN 'Endotracheal tube'
                                WHEN source.tracheostomy_tube_insertion_date IS NOT NULL
                                THEN 'Tracheostomy tube'
                                ELSE NULL
                            END AS device_source_value,
                         
                            32879 AS device_type_concept_id, -- Registry concept id
                            1 AS quantity,
                            source.session_id AS visit_occurrence_id
                        FROM {source_schema}.icu AS source
                        JOIN {omop_sqldev_schema}.person AS cdm
                            ON source.anon_case_no=cdm.person_source_value
                        WHERE
                            source.endotracheal_tube_insertion_date IS NOT NULL OR
                            source.tracheostomy_tube_insertion_date IS NOT NULL -- Filter rows where both insertion dates are NULL
                        
                        UNION ALL
                 
                        SELECT DISTINCT
                            ROW_NUMBER() OVER (ORDER BY s.id, session_startdate) + (
                                SELECT COALESCE(MAX(device_exposure_id), 0)
                                FROM {omop_schema}.stg__device_exposure) AS device_exposure_id,
                        
                            cdm.person_id AS person_id,
                            cdm.person_source_value AS person_source_value,
                        
                            2616666 AS device_concept_id,
                        
                            s.session_startdate AS device_exposure_start_date,
                            null AS device_exposure_end_date,
                        
                            'CPAP' AS device_source_value,
                        
                            32879 AS device_type_concept_id, -- Registry concept id
                            1 AS quantity,
                            s.session_id AS visit_occurrence_id
                        FROM preop.riskindex AS s
                        JOIN {omop_sqldev_schema}.person AS cdm
                            ON s.anon_case_no = cdm.person_source_value
                        WHERE
                            s.cpap_use LIKE 'Yes%'
                        '''
                     ))

                # Ingest from stg__device_exposure into OMOP device_exposure Table
                # connection.execute(
                #     text(f'''
                #         INSERT INTO {omop_schema}.device_exposure (
                #             device_exposure_id,
                #             person_id,
                #             device_concept_id,
                #             device_exposure_start_date,
                #             device_exposure_start_datetime,
                #             device_exposure_end_date,
                #             device_exposure_end_datetime,
                #             device_type_concept_id,
                #             unique_device_id,
                #             production_id,
                #             quantity,
                #             provider_id,
                #             visit_occurrence_id,
                #             visit_detail_id,
                #             device_source_value,
                #             device_source_concept_id,
                #             unit_concept_id,
                #             unit_source_value,
                #             unit_source_concept_id
                #         )
                #         SELECT
                #             device_exposure_id,
                #             person_id,
                #             device_concept_id,
                #             device_exposure_start_date,
                #             NULL AS device_exposure_start_datetime,
                #             device_exposure_end_date,
                #             NULL AS device_exposure_end_datetime,
                #             device_type_concept_id,
                #             NULL AS unique_device_id,
                #             NULL AS production_id,
                #             quantity,
                #             provider_id,
                #             visit_occurrence_id,
                #             NULL AS visit_detail_id,
                #             device_source_value,
                #             NULL AS device_source_concept_id,
                #             NULL AS unit_concept_id,
                #             NULL AS unit_source_value,
                #             NULL AS unit_source_concept_id
                #         FROM {omop_schema}.stg__device_exposure'''
                #      ))

    def finalize(self):
        # Verify if needed
        pass
