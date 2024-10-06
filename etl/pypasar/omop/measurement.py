import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
import pandas as pd
# Load environment variables from the .env file
load_dotenv()


class measurement:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.source_preop_schema = os.getenv("POSTGRES_SOURCE_PREOP_SCHEMA")
        self.source_intraop_schema = os.getenv("POSTGRES_SOURCE_INTRAOP_SCHEMA")
        self.source_postop_schema = os.getenv("POSTGRES_SOURCE_POSTOP_SCHEMA")
        self.temp_table = f"temp_measurement_preop_lab_{os.urandom(15).hex()}"
        self.source_tables_cols = [{"table": f"{self.source_preop_schema}.lab", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "preop_lab_test_description": str,
                                              "preop_lab_result_value": float, 
                                              "preop_lab_collection_datetime": "datetime64[ns]"}},
                                   {"table": f"{self.source_preop_schema}.char", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "height": float, "weight": float, "bmi": float, "systolic_bp": float,
                                              "diastolic_bp": float, "heart_rate": float,
                                              "o2_saturation": float, "o2_supplementaries": str,
                                              "temperature": float, "pain_score": float}}, 
                                   {"table": f"{self.source_intraop_schema}.aimsvitals", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "vitalcode": str,
                                              "vital_num_value": float, 
                                              "vitaldt": "datetime64[ns]", "vital_date": "datetime64[ns]", "vital_time": str}}, 
                                   {"table": f"{self.source_intraop_schema}.operation", 
                                    "columns": {"anon_case_no": str, "vital_code": str,
                                              "vital_signs_result": float, "vital_signs_taken_datetime": "datetime64[ns]",
                                              "vital_signs_taken_date": "datetime64[ns]", 
                                              "vital_signs_taken_time": str}}, 
                                   {"table": f"{self.source_postop_schema}.lab", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, 
                                              "postop_lab_collection_datetime_max": "datetime64[ns]",
                                              "postop_lab_collection_datetime_min": "datetime64[ns]", 
                                              "postop_lab_test_desc": str,
                                              "postop_result_value_max": float, "postop_result_value_min": float}}, 
                                   {"table": f"{self.source_postop_schema}.labsall", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "gen_lab_lab_test_code": str,
                                              "gen_lab_result_value": str, "gen_lab_specimen_collection_date": "datetime64[ns]",
                                              "gen_lab_specimen_collection_time": str}},
                                   {"table": f"{self.source_preop_schema}.others", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "asa_score_aims": float, "asa_score_eaf": str,
                                              "efs_total_score": int}}, 
                                   {"table": f"{self.source_preop_schema}.riskindex", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "asa_class": str, "cri_functional_status": str,
                                              "cardiac_risk_index": float, "cardiac_risk_class": str,
                                              "osa_risk_index": str, "act_risk": str}}, 
                                   {"table": f"{self.source_intraop_schema}.nurvitals", 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "authored_datetime": "datetime64[ns]", "document_item_desc": str, "document_item_right_label": str, "value_text": str}}]

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
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f"Truncate table {self.omop_schema}.measurement"))

    def process(self):
        for source_table_cols in self.source_tables_cols:
            print(source_table_cols)
            self.limit, self.offset = int(os.getenv("PROCESSING_BATCH_SIZE")), 0
            self.process_by_source_table(source_table_cols)
            # break

    def process_by_source_table(self, source_table_cols):
        print(f"Processing {source_table_cols['table']}..")
        total_count_source_table = self.fetch_total_count_source_table(source_table_cols['table'])
        print(f"Total count {total_count_source_table}")
        while self.offset <= total_count_source_table: # Fetch and process in batches
        source_batch = self.retrieve(source_table_cols)
            transformed_batch = self.transform(source_batch)
            del source_batch
            self.ingest(transformed_batch)
            del transformed_batch
            gc.collect()
            self.offset = self.offset + self.limit
            # break


    def fetch_total_count_source_table(self, source_table_name):
        with self.engine.connect() as connection:
            with connection.begin():
                res = connection.execute(text(f"select count(1) from {source_table_name}"))
                return res.first()[0]

    def retrieve(self, source_table_cols):
        source_batch = self.fetch_in_batch_source_table(source_table_cols)
        source_df = pd.DataFrame(source_batch.fetchall())
        source_df.columns = source_table_cols["columns"].keys()
        print(source_df.head(1))
        print(f"offset {self.offset} limit {self.limit} batch_count {len(source_df)} for {source_table_cols['table']} retrieved..")
        return source_df

    def fetch_in_batch_source_table(self, source_table_cols):
        # Formulate columns and table
        select_sql = "SELECT "
        source_columns = list(source_table_cols["columns"])
        for idx, col in enumerate(source_columns):
            if idx < len(source_columns) - 1:
                select_sql += f"{col},"
            else:
                select_sql += f"{col}"
        select_sql += f" FROM {source_table_cols['table']} LIMIT {self.limit} OFFSET {self.offset}"
        #print(select_sql)
        with self.engine.connect() as connection:
            with connection.begin():
                res = connection.execute(text(select_sql))
                return res

    def transform(self, source_batch):
        condition_occurrence_schema = {
            "condition_occurrence_id": int,
            "person_id": int,
            "condition_concept_id": int,
            "condition_start_date": "datetime64[ns]",
            "condition_start_datetime": "datetime64[ns]",
            "condition_end_date": "datetime64[ns]",
            "condition_end_datetime": "datetime64[ns]",
            "condition_type_concept_id": int,
            "condition_status_concept_id": int,
            "visit_occurrence_id": int,
            "condition_source_value": str
        }
        # Initialize dataframe and display columns info
        condition_occ_df = pd.DataFrame(columns=condition_occurrence_schema.keys()).astype(condition_occurrence_schema)
        # print(f"source {len(source_batch)}")
        
        if len(source_batch) > 0:
            condition_occ_df["condition_start_date"] = pd.to_datetime(source_batch["diagnosis_date"]).dt.date
            condition_occ_df["condition_start_datetime"] = source_batch["diagnosis_date"]
            condition_occ_df["condition_end_date"] = None
            condition_occ_df["condition_end_datetime"] = None
            condition_occ_df["condition_type_concept_id"] = 32879
            condition_occ_df["condition_status_concept_id"] = 32896
            condition_occ_df["condition_concept_id"] = 0 # TODO: Update
            condition_occ_df["person_id"] = 0 # TODO: Update
            condition_occ_df["visit_occurrence_id"] = 0 # TODO: Update
            condition_occ_df["condition_source_value"] = source_batch["diagnosis_code"]
            condition_occ_df["condition_occurrence_id"] = range(self.offset + 1, self.offset + len(source_batch) + 1)
            #print(condition_occ_df.head(1))
        
        # print(f"target {len(condition_occ_df)}")
        return condition_occ_df


    def ingest(self, transformed_batch):
        transformed_batch.to_sql(name="condition_occurrence", schema=self.omop_schema, con=self.engine, if_exists="append", index=False)
        print(f"offset {self.offset} limit {self.limit} batch_count {len(transformed_batch)} ingested..")

    def finalize(self):
        # cleanup
        self.engine.dispose()

