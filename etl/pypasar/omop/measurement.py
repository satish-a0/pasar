import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
import pandas as pd
import gc
from enum import Enum
from datetime import datetime
# Load environment variables from the .env file
load_dotenv()

class measurement():

    def __init__(self):
        self.source = Enum(value='Source', names=[("PREOP_LAB", f"{os.getenv('POSTGRES_SOURCE_PREOP_SCHEMA')}.lab"), 
                                                  ("PREOP_CHAR", f"{os.getenv('POSTGRES_SOURCE_PREOP_SCHEMA')}.char"),
                                                  ("INTRAOP_AIMSVITALS", f"{os.getenv('POSTGRES_SOURCE_INTRAOP_SCHEMA')}.aimsvitals"),
                                                  ("INTRAOP_OPERATION", f"{os.getenv('POSTGRES_SOURCE_INTRAOP_SCHEMA')}.operation"),
                                                  ("POSTOP_LAB", f"{os.getenv('POSTGRES_SOURCE_POSTOP_SCHEMA')}.lab"),
                                                  ("POSTOP_LABSALL", f"{os.getenv('POSTGRES_SOURCE_POSTOP_SCHEMA')}.labsall"),
                                                  ("PREOP_OTHERS", f"{os.getenv('POSTGRES_SOURCE_PREOP_SCHEMA')}.others"),
                                                  ("PREOP_RISKINDEX", f"{os.getenv('POSTGRES_SOURCE_PREOP_SCHEMA')}.riskindex"),
                                                  ("INTRAOP_NURVITALS", f"{os.getenv('POSTGRES_SOURCE_INTRAOP_SCHEMA')}.nurvitals")])
        self.engine = postgres().get_engine()  # Get PG Connection
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.source_preop_schema = os.getenv("POSTGRES_SOURCE_PREOP_SCHEMA")
        self.source_intraop_schema = os.getenv("POSTGRES_SOURCE_INTRAOP_SCHEMA")
        self.source_postop_schema = os.getenv("POSTGRES_SOURCE_POSTOP_SCHEMA")
        self.temp_table = f"temp_measurement_preop_lab_{os.urandom(15).hex()}"
        self.measurement_id_start = 1
        self.source_tables_cols = [{"table": self.source.PREOP_LAB.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "preop_lab_test_description": str,
                                              "preop_lab_result_value": float, 
                                              "preop_lab_collection_datetime": "datetime64[ns]"}},
                                   {"table": self.source.PREOP_CHAR.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "height": float, "weight": float, "bmi": float, "systolic_bp": float,
                                              "diastolic_bp": float, "heart_rate": float,
                                              "o2_saturation": float, "o2_supplementaries": str,
                                              "temperature": float, "pain_score": float}}, 
                                   {"table": self.source.INTRAOP_AIMSVITALS.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "vitalcode": str,
                                              "vital_num_value": float, 
                                              "vitaldt": "datetime64[ns]", "vital_date": "datetime64[ns]", "vital_time": str}}, 
                                   {"table": self.source.INTRAOP_OPERATION.value, 
                                    "columns": {"anon_case_no": str, "vital_code": str,
                                              "vital_signs_result": float, "vital_signs_taken_datetime": "datetime64[ns]",
                                              "vital_signs_taken_date": "datetime64[ns]", 
                                              "vital_signs_taken_time": str}}, 
                                   {"table": self.source.POSTOP_LAB.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, 
                                              "postop_lab_collection_datetime_max": "datetime64[ns]",
                                              "postop_lab_collection_datetime_min": "datetime64[ns]", 
                                              "postop_lab_test_desc": str,
                                              "postop_result_value_max": float, "postop_result_value_min": float}}, 
                                   {"table": self.source.POSTOP_LABSALL.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "gen_lab_lab_test_code": str,
                                              "gen_lab_result_value": str, "gen_lab_specimen_collection_date": "datetime64[ns]",
                                              "gen_lab_specimen_collection_time": str}},
                                   {"table": self.source.PREOP_OTHERS.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "asa_score_aims": float, "asa_score_eaf": str,
                                              "efs_total_score": int}}, 
                                   {"table": self.source.PREOP_RISKINDEX.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "asa_class": str, "cri_functional_status": str,
                                              "cardiac_risk_index": float, "cardiac_risk_class": str,
                                              "osa_risk_index": str, "act_risk": str}}, 
                                   {"table": self.source.INTRAOP_NURVITALS.value, 
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
            # if source_table_cols["table"] in [f"{self.source_preop_schema}.riskindex"]:
                print(source_table_cols)
                self.limit, self.offset = int(os.getenv("PROCESSING_BATCH_SIZE")), 0
                self.process_by_source_table(source_table_cols)
                print(f"{source_table_cols['table']} processing completed..")

    def process_by_source_table(self, source_table_cols):
        print(f"Processing {source_table_cols['table']}..")
        total_count_source_table = self.fetch_total_count_source_table(source_table_cols['table'])
        print(f"Total count {total_count_source_table}")
        while self.offset <= total_count_source_table: # Fetch and process in batches
            source_batch = self.retrieve(source_table_cols)
            print(f"measurement id start: {self.measurement_id_start}")
            transformed_batch = self.transform(source_table_cols, source_batch)
            self.measurement_id_start += len(source_batch)
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
        select_sql += f" FROM {source_table_cols['table']} order by anon_case_no LIMIT {self.limit} OFFSET {self.offset}"
        # select_sql += f" FROM {source_table_cols['table']} order by anon_case_no LIMIT 2"
        #print(select_sql)
        with self.engine.connect() as connection:
            with connection.begin():
                res = connection.execute(text(select_sql))
                return res

    def transform(self, source_table_cols, source_batch):
        measurement_schema = {
            "measurement_id": int,
            "person_id": int,
            "measurement_concept_id": int,
            "measurement_type_concept_id": int,
            "measurement_date": "datetime64[ns]",
            "measurement_datetime": "datetime64[ns]",
            "measurement_time": str,
            "value_as_number": object,
            "measurement_source_value": str,
            "value_source_value": str,
            "visit_occurrence_id": int,
            "unit_source_value": str
        }
        # Initialize dataframe and display columns info
        measurement_df = pd.DataFrame(columns=measurement_schema.keys()).astype(measurement_schema)

        match source_table_cols["table"]:
            case self.source.PREOP_LAB.value:
                return self.transform_preop_lab(source_table_cols, source_batch, measurement_df)
            case self.source.PREOP_CHAR.value:
                return self.transform_preop_char(source_table_cols, source_batch, measurement_df)
            case self.source.INTRAOP_AIMSVITALS.value:
                return self.transform_intraop_aimsvitals(source_table_cols, source_batch, measurement_df)
            case self.source.INTRAOP_OPERATION.value:
                return self.transform_intraop_operation(source_table_cols, source_batch, measurement_df)
            case self.source.POSTOP_LAB.value:
                return self.transform_postop_lab(source_table_cols, source_batch, measurement_df)
            case self.source.POSTOP_LABSALL.value:
                return self.transform_postop_labsall(source_table_cols, source_batch, measurement_df)
            case self.source.PREOP_OTHERS.value:
                return self.transform_preop_others(source_table_cols, source_batch, measurement_df)
            case self.source.PREOP_RISKINDEX.value:
                return self.transform_preop_riskindex(source_table_cols, source_batch, measurement_df)
            case self.source.INTRAOP_NURVITALS.value:
                return self.transform_intraop_nurvitals(source_table_cols, source_batch, measurement_df)
            case _:
                raise ValueError(f"Invalid source table {source_table_cols['table']}")

    def transform_preop_lab(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_lab..")
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
            measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id for "preop_lab_result_value"
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        print(measurement_df.head(100))
        return measurement_df

    def transform_preop_char(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_char..")
        # 1 to Many
        measurement_score_columns = ["height","weight","bmi", "systolic_bp", "diastolic_bp", "heart_rate", "o2_saturation", "temperature", "pain_score"]
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["session_startdate"]).dt.date
            measurement_df["measurement_type_concept_id"] = 32879
            value_as_number_df = source_batch[measurement_score_columns] # Assumption Ignoring "o2_supplementaries" since its an additional value for the o2_saturation
            measurement_df["value_as_number"] = value_as_number_df.apply(lambda row: row.tolist(), axis=1)
            measurement_df["measurement_source_value"] = [measurement_score_columns]*len(measurement_df)
            # value_as_source_df = source_batch[["o2_supplementaries"]]
            # measurement_df["value_source_value"] = value_as_source_df.apply(lambda row: row.tolist(), axis=1)

            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update 
            measurement_df["person_id"] = 0 # TODO: Update
            
            measurement_df = measurement_df.explode(['value_as_number', 'measurement_source_value'])
            measurement_df = measurement_df.dropna(subset=['value_as_number'], thresh=1)
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            print(measurement_df.head(len(measurement_df)))
        return measurement_df

    def transform_intraop_aimsvitals(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_aimsvitals..")
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["vital_date"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["vitaldt"]
            measurement_df["measurement_time"] = source_batch["vital_time"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["vital_num_value"]
            measurement_df["measurement_source_value"] = source_batch["vitalcode"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id for "vital_num_value"
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df

    def transform_intraop_operation(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_operation..")
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["vital_signs_taken_date"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["vital_signs_taken_datetime"]
            measurement_df["measurement_time"] = source_batch["vital_signs_taken_time"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["vital_signs_result"]
            measurement_df["measurement_source_value"] = source_batch["vital_code"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id for "preop_lab_result_value"
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        print(measurement_df.head(1))
        return measurement_df

    def transform_postop_lab(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_postop_lab..")
        # Assumption picking only max values for simplicity and ignoring the min values
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["postop_lab_collection_datetime_max"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["postop_lab_collection_datetime_max"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["postop_result_value_max"]
            measurement_df["measurement_source_value"] = source_batch["postop_lab_test_desc"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id for "postop_lab_test_desc"
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        print(measurement_df.head(1))
        return measurement_df


    def transform_postop_labsall(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_postop_labsall..")
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["gen_lab_specimen_collection_date"]).dt.date
            measurement_df["measurement_datetime"] = source_batch[['gen_lab_specimen_collection_date','gen_lab_specimen_collection_time']].astype(str).apply(lambda x: datetime.strptime(x.gen_lab_specimen_collection_date + x.gen_lab_specimen_collection_time, '%Y-%m-%d%H:%M:%S'), axis=1)
            measurement_df["measurement_time"] = source_batch["gen_lab_specimen_collection_time"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["gen_lab_result_value"]
            measurement_df["measurement_source_value"] = source_batch["gen_lab_lab_test_code"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id for "gen_lab_lab_test_code"
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        print(measurement_df.head(1))
        return measurement_df

    def transform_preop_others(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_others..")
        # 1 to Many
        measurement_score_columns = ["efs_total_score","asa_score_aims","asa_score_eaf"]
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["session_startdate"]).dt.date
            measurement_df["measurement_type_concept_id"] = 32879
            value_as_number_df = source_batch[measurement_score_columns] # Assumption Ignoring "o2_supplementaries" since its an additional value for the o2_saturation
            measurement_df["value_as_number"] = value_as_number_df.apply(lambda row: row.tolist(), axis=1)
            measurement_df["measurement_source_value"] = [measurement_score_columns]*len(measurement_df)
            # value_as_source_df = source_batch[["o2_supplementaries"]]
            # measurement_df["value_source_value"] = value_as_source_df.apply(lambda row: row.tolist(), axis=1)

            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update 
            measurement_df["person_id"] = 0 # TODO: Update
            
            measurement_df = measurement_df.explode(['value_as_number', 'measurement_source_value'])
            measurement_df = measurement_df.dropna(subset=['value_as_number'], thresh=1)
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            print(measurement_df.head(len(measurement_df)))
        return measurement_df

    def transform_preop_riskindex(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_riskindex..")
        # 1 to Many
        # Assumption adding cardiac_risk_index as part of value_source_value instead of value_as_number for simplicity
        measurement_score_columns = ["asa_class","cri_functional_status","cardiac_risk_index","cardiac_risk_class",         "osa_risk_index","act_risk"]
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["session_startdate"]).dt.date
            measurement_df["measurement_type_concept_id"] = 32879
            value_as_source_df = source_batch[measurement_score_columns] # Assumption Ignoring "o2_supplementaries" since its an additional value for the o2_saturation
            measurement_df["value_source_value"] = value_as_source_df.apply(lambda row: row.tolist(), axis=1)
            measurement_df["measurement_source_value"] = [measurement_score_columns]*len(measurement_df)

            measurement_df["measurement_concept_id"] = 0 # TODO: Update Vocab concept id
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update 
            measurement_df["person_id"] = 0 # TODO: Update
            
            measurement_df = measurement_df.explode(['value_source_value', 'measurement_source_value'])
            measurement_df = measurement_df.dropna(subset=['value_source_value'], thresh=1)
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            print(measurement_df.head(len(measurement_df)))
        return measurement_df

    def transform_intraop_nurvitals(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_nurvitals..")
        if len(source_batch) > 0:
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["authored_datetime"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["authored_datetime"]
            measurement_df["measurement_source_value"] = source_batch["document_item_desc"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

            measurement_df["value_source_value"] = source_batch["value_text"] # Contains mix of text and numeric. Could replicate the numeric values to othe column value_as_number based on the type of document_item_desc/measurement_source_value
            measurement_df["measurement_concept_id"] = 0 # TODO: Update
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        print(measurement_df.head(1))
        return measurement_df

    def ingest(self, transformed_batch):
        transformed_batch.to_sql(name="measurement", schema=self.omop_schema, con=self.engine, if_exists="append", index=False)
        print(f"offset {self.offset} limit {self.limit} batch_count {len(transformed_batch)} ingested..")

    def finalize(self):
        # cleanup
        self.engine.dispose()

