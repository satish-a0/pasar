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
                connection.execute(text("DROP VIEW IF EXISTS stg__device_exposure CASCADE"))

                # Drop int__device_exposure view if exists
                connection.execute(text("DROP VIEW IF EXISTS int__device_exposure CASCADE"))

                # Delete device_exposure table
                connection.execute(text("DELETE FROM device_exposure"))

    def process(self):
        # In batches
        omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        postop_schema = os.getenv("POSTGRES_SOURCE_POSTOP_SCHEMA")
        preop_schema = os.getenv("POSTGRES_SOURCE_PREOP_SCHEMA")

        # Read from source
        with self.engine.connect() as connection:
            with connection.begin():
        # Transform
                connection.execute(
                    text(f'''
                        -- Create or replace the staging view for the visit_detail table
                        CREATE OR REPLACE VIEW {omop_schema}.stg__device_exposure AS
                            -- Extract relevant columns from the post_op.icu table
                            WITH postop__icu AS (
                                SELECT
                                    id,
                                    anon_case_no,
                                    session_id,
                                    -- Handle device_concept_id
                                    CASE
                                        WHEN icu.endotracheal_tube_insertion_date IS NOT NULL 
                                        THEN 4097216
                                        ELSE 4044008
                                    END AS device_concept_id,
                                    COALESCE(icu.endotracheal_tube_insertion_date, icu.tracheostomy_tube_insertion_date) AS start_date,
                                    COALESCE(icu.endotracheal_tube_removal_date, icu.tracheostomy_tube_removal_date) AS end_date,
                                    CASE
                                        WHEN icu.endotracheal_tube_insertion_date IS NOT NULL 
                                        THEN 'Endotracheal tube'
                                        ELSE 'Tracheostomy tube'
                                    END AS device_source_value,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY anon_case_no, session_id, 
                                        CASE
                                            WHEN icu.endotracheal_tube_insertion_date IS NOT NULL 
                                            THEN 4097216
                                            ELSE 4044008
                                        END,
                                        COALESCE(icu.endotracheal_tube_insertion_date, icu.tracheostomy_tube_insertion_date),
                                        COALESCE(icu.endotracheal_tube_removal_date, icu.tracheostomy_tube_removal_date),
                                        CASE
                                            WHEN icu.endotracheal_tube_insertion_date IS NOT NULL 
                                            THEN 'Endotracheal tube'
                                            ELSE 'Tracheostomy tube'
                                        END
                                        ORDER BY id
                                    ) AS row_num
                                FROM {postop_schema}.icu
                                WHERE endotracheal_tube_insertion_date IS NOT NULL OR
                                        tracheostomy_tube_insertion_date IS NOT NULL -- Filter rows where both insertion dates are NULL
                            ),
                            preop__riskindex AS (
                                            SELECT DISTINCT
                                                id, 
                                                anon_case_no,
                                                session_id,
                                                2616666 AS device_concept_id,
                                                session_startdate AS start_date,
                                                CAST(NULL AS DATE) AS end_date,
                                                'CPAP' AS device_source_value,
                                                ROW_NUMBER() OVER (
                                                    PARTITION BY anon_case_no, session_id, 2616666,
                                                    session_startdate, CAST(NULL AS DATE),
                                                    'CPAP'
                                                    ORDER BY id
                                                ) AS row_num
                                            FROM {preop_schema}.riskindex
                                            WHERE cpap_use LIKE 'Yes%'
                            ),
                            -- Ensure only distinct rows with corresponding id and DE_start_date
                            filtered_combined AS (
                                SELECT *
                                FROM postop__icu
                                WHERE row_num = 1

                                UNION ALL

                                SELECT *
                                FROM preop__riskindex
                                WHERE row_num = 1
                            ),
                            -- Finalized the staging table
                            final AS (
                                SELECT 
                                    com.id AS id,
                                    com.anon_case_no AS anon_case_no,
                                    com.session_id AS session_id,
                                    com.device_concept_id as device_concept_id,
                                    com.start_date AS device_exposure_start_date,
                                    com.end_date AS device_exposure_end_date,
                                    com.device_source_value AS device_source_value
                                FROM filtered_combined AS com
                            )

                            SELECT
                                id,                         
                                anon_case_no,    
                                device_concept_id,                      
                                device_exposure_start_date,
                                device_exposure_end_date,
                                device_source_value,        
                                session_id                      
                            FROM final;
                        '''
                     ))

                connection.execute(
                    text(f'''
                        -- Create intermediate view
                        CREATE OR REPLACE VIEW {omop_schema}.int__device_exposure AS
                            -- Convert visit_occurrence_id back to session_id
                            WITH session__id AS (
                                SELECT CAST(LEFT(CAST(visit_occurrence_id AS TEXT), LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2) AS INTEGER) AS session_id, *
                                FROM {omop_schema}.visit_occurrence
                            ),
                            -- Combine with other dimension tables
                            final AS (
                                SELECT
                                    p.person_id AS person_id,
                                    stg__de.device_concept_id AS device_concept_id,
                                    stg__de.device_exposure_start_date AS device_exposure_start_date,
                                    stg__de.device_exposure_end_date AS device_exposure_end_date,
                                    stg__de.device_source_value AS device_source_value,
                                    v.visit_occurrence_id AS visit_occurrence_id,
                                    stg__de.id AS id
                                FROM {omop_schema}.stg__device_exposure AS stg__de
                                LEFT JOIN {omop_schema}.person AS p
                                    ON stg__de.anon_case_no = p.person_source_value
                                LEFT JOIN session__id AS v
                                    ON stg__de.session_id = v.session_id
                            )

                            SELECT
                                person_id,
                                device_concept_id,
                                device_exposure_start_date,
                                device_exposure_end_date,
                                device_source_value,
                                visit_occurrence_id,
                                id          
                            FROM final;
                        '''
                     ))

                # Ingest from stg__device_exposure into OMOP device_exposure Table
                connection.execute(
                    text(f'''
                        INSERT INTO {omop_schema}.device_exposure (
                            device_exposure_id,
                            person_id,
                            device_concept_id,
                            device_exposure_start_date,
                            device_exposure_start_datetime,
                            device_exposure_end_date,
                            device_exposure_end_datetime,
                            device_type_concept_id,
                            unique_device_id,
                            production_id,
                            quantity,
                            provider_id,
                            visit_occurrence_id,
                            visit_detail_id,
                            device_source_value,
                            device_source_concept_id,
                            unit_concept_id,
                            unit_source_value,
                            unit_source_concept_id
                        )
                        SELECT
                            -- Autogenerate unique device_exposure_id based on device_exposure_start_date and id
                            ROW_NUMBER() OVER (ORDER BY device_exposure_start_date, id) AS device_exposure_id,
                            person_id,
                            device_concept_id,
                            device_exposure_start_date,
                            NULL AS device_exposure_start_datetime,
                            device_exposure_end_date,
                            NULL AS device_exposure_end_datetime,
                            32879 AS device_type_concept_id, -- Registry concept id
                            NULL AS unique_device_id,
                            NULL AS production_id,
                            NULL AS quantity,
                            NULL AS provider_id,
                            visit_occurrence_id,
                            NULL AS visit_detail_id,
                            device_source_value,
                            NULL AS device_source_concept_id,
                            NULL AS unit_concept_id,
                            NULL AS unit_source_value,
                            NULL AS unit_source_concept_id
                        FROM {omop_schema}.int__device_exposure'''
                     ))

    def finalize(self):
        # Verify if needed
        pass
