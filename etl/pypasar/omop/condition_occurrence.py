import traceback
import os
import gc
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
import pandas as pd
# Load environment variables from the .env file
load_dotenv()


class condition_occurrence:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.source_postop_schema = os.getenv("POSTGRES_SOURCE_POSTOP_SCHEMA")
        self.limit, self.offset = int(os.getenv("PROCESSING_BATCH_SIZE")), 0
        self.temp_table = f'temp_condition_occurrence_{os.urandom(15).hex()}'

    def execute(self):
        try:
            self.initialize()
            self.process()
            self.finalize()
        except Exception as err:
            print(f"Error occurred {self.__class__.__name__}")
            raise err

    def initialize(self):
        # Truncate
        self.truncate_table(f"{self.omop_schema}.condition_occurrence")

    def process(self):
        total_count_source_postop_discharge = self.fetch_total_count_source_postop_discharge()
        print(f"Total count {total_count_source_postop_discharge}")
        while self.offset <= total_count_source_postop_discharge: # Fetch and process in batches
            source_batch = self.retrieve()
            transformed_batch = self.transform(source_batch)
            del source_batch
            self.ingest(transformed_batch)
            del transformed_batch
            gc.collect()
            self.truncate_table(f"{self.omop_schema}.{self.temp_table}") # Truncate temp table
            self.offset = self.offset + self.limit

    def retrieve(self):
        source_batch = self.fetch_in_batch_source_postop_discharge()
        source_postop_discharge_df = pd.DataFrame(source_batch.fetchall())
        source_postop_discharge_df.columns = {'anon_case_no': str, 'id': int, 'diagnosis_date': 'datetime64[ns]', 'diagnosis_code': str, 'session_id': int}.keys()
        print(source_postop_discharge_df.head(1))
        print(f"offset {self.offset} limit {self.limit} batch_count {len(source_postop_discharge_df)} retrieved..")
        return source_postop_discharge_df
    
    def transform(self, source_batch):
        condition_occurrence_schema = {
            'condition_occurrence_id': int,
            'person_id': int,
            'condition_concept_id': int,
            'condition_start_date': 'datetime64[ns]',
            'condition_start_datetime': 'datetime64[ns]',
            'condition_end_date': 'datetime64[ns]',
            'condition_end_datetime': 'datetime64[ns]',
            'condition_type_concept_id': int,
            'condition_status_concept_id': int,
            'visit_occurrence_id': int,
            'condition_source_value': str
        }
        # Initialize dataframe and display columns info
        condition_occ_df = pd.DataFrame(columns=condition_occurrence_schema.keys()).astype(condition_occurrence_schema)
        # print(f"source {len(source_batch)}")
        
        if len(source_batch) > 0:
            condition_occ_df['condition_start_date'] = pd.to_datetime(source_batch['diagnosis_date']).dt.date
            condition_occ_df['condition_start_datetime'] = source_batch['diagnosis_date']
            condition_occ_df['condition_type_concept_id'] = 32879
            condition_occ_df['condition_status_concept_id'] = 32896
            condition_occ_df['condition_concept_id'] = 0 # TODO: Update
            condition_occ_df['anon_case_no'] = source_batch['anon_case_no'] # Person source value
            condition_occ_df['session_id'] = source_batch['session_id'] # Visit occurrence id source value without suffix
            condition_occ_df['condition_source_value'] = source_batch['diagnosis_code']
            condition_occ_df['condition_occurrence_id'] = range(self.offset + 1, self.offset + len(source_batch) + 1)
            #print(condition_occ_df.head(1))
        
        # print(f"target {len(condition_occ_df)}")
        return condition_occ_df


    def ingest(self, transformed_batch):
        transformed_batch.to_sql(name=self.temp_table, schema=self.omop_schema, con=self.engine, if_exists='replace', index=False)
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f'SET search_path TO {self.omop_schema}'))
                connection.execute(text(f'''INSERT INTO condition_occurrence (condition_occurrence_id, 
                                             person_id, 
                                             condition_concept_id, 
                                             condition_type_concept_id, 
                                             condition_status_concept_id, 
                                             condition_start_date, 
                                             condition_start_datetime, 
                                             visit_occurrence_id,
                                             condition_source_value) 
                                             SELECT t.condition_occurrence_id, 
                                                    p.person_id, 
                                                    t.condition_concept_id, 
                                                    t.condition_type_concept_id, 
                                                    t.condition_status_concept_id, 
                                                    t.condition_start_date, 
                                                    t.condition_start_datetime, 
                                                    v.visit_occurrence_id,
                                                    t.condition_source_value 
                                                       FROM {self.temp_table} t 
                                                       INNER JOIN PERSON p ON t.anon_case_no = p.person_source_value
                                                       INNER JOIN 
                                                            (SELECT visit_occurrence_id, 
						                                       row_number() over (
												                   partition by CAST(LEFT(CAST(visit_occurrence_id AS TEXT), LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2) AS INTEGER)
											                    ) rownum,
										                        CAST(LEFT(CAST(visit_occurrence_id AS TEXT), LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2) AS INTEGER) AS truncated_visit_occurrence_id 
									                        FROM VISIT_OCCURRENCE) v 
                                                        ON v.truncated_visit_occurrence_id  = t.session_id AND v.rownum = 1''')) # Temporary solution to just pick the 1st row
        print(f"offset {self.offset} limit {self.limit} batch_count {len(transformed_batch)} ingested..")

    def fetch_total_count_source_postop_discharge(self):
        with self.engine.connect() as connection:
            with connection.begin():
                res = connection.execute(text(f'select count(1) from {self.source_postop_schema}.discharge'))
                return res.first()[0]

    def fetch_in_batch_source_postop_discharge(self):
        # TODO: Fetch person_id from person table, visit_occurrence_id from visit_occurrence
        with self.engine.connect() as connection:
            with connection.begin():
                res = connection.execute(
                    text(f'select anon_case_no, id, diagnosis_date, diagnosis_code, session_id from {self.source_postop_schema}.discharge limit {self.limit} offset {self.offset}'))
                return res

    def truncate_table(self, table_name_w_schema_prefix):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f"Truncate table {table_name_w_schema_prefix}"))

    def drop_table(self, table_name_w_schema_prefix):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f"DROP table {table_name_w_schema_prefix}"))

    def finalize(self):
        # cleanup
        self.drop_table(f"{self.omop_schema}.{self.temp_table}")
        self.engine.dispose()
