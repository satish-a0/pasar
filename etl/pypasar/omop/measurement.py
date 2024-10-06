import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
import pandas as pd
import gc
from enum import Enum
# Load environment variables from the .env file
load_dotenv()

class measurement():

    def __init__(self):
        self.Source = Enum(value='Source', names=[("PREOP_LAB", f"{os.getenv('POSTGRES_SOURCE_PREOP_SCHEMA')}.lab"), 
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
        self.measurement_id_start, self.measurement_id_end = 1, 0
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
            if source_table_cols["table"] in [f"{self.source_preop_schema}.lab", f"{self.source_intraop_schema}.aimsvitals"]:
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
            self.measurement_id_end += len(source_batch)
            #print(f"measurement id start: {self.measurement_id_start} measurement id end: {self.measurement_id_end}")
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
        select_sql += f" FROM {source_table_cols['table']} LIMIT {self.limit} OFFSET {self.offset}"
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
            "value_as_number": float,
            "measurement_source_value": str,
            "value_source_value": str,
            "visit_occurrence_id": int,
            "unit_source_value": str
        }
        # Initialize dataframe and display columns info
        measurement_df = pd.DataFrame(columns=measurement_schema.keys()).astype(measurement_schema)

        match source_table_cols["table"]:
            case self.Source.PREOP_LAB.value:
                return self.transform_preop_lab(source_table_cols, source_batch, measurement_df)
            case self.Source.PREOP_CHAR.value:
                return self.transform_preop_char(source_table_cols, source_batch, measurement_df)
            case self.Source.INTRAOP_AIMSVITALS.value:
                return self.transform_intraop_aimsvitals(source_table_cols, source_batch, measurement_df)
            case self.Source.INTRAOP_OPERATION.value:
                return self.transform_intraop_operation(source_table_cols, source_batch, measurement_df)
            case self.Source.POSTOP_LAB.value:
                return self.transform_postop_lab(source_table_cols, source_batch, measurement_df)
            case self.Source.POSTOP_LABSALL.value:
                return self.transform_postop_labsall(source_table_cols, source_batch, measurement_df)
            case self.Source.PREOP_OTHERS.value:
                return self.transform_preop_others(source_table_cols, source_batch, measurement_df)
            case self.Source.PREOP_RISKINDEX.value:
                return self.transform_preop_riskindex(source_table_cols, source_batch, measurement_df)
            case self.Source.INTRAOP_NURVITALS.value:
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
            measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

            measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
            measurement_df["measurement_concept_id"] = 0 # TODO: Update
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df

    def transform_preop_char(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_char..")
        # if len(source_batch) > 0:
        #     measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
        #     measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
        #     measurement_df["measurement_type_concept_id"] = 32879
        #     measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
        #     measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
        #     measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

        #     measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
        #     measurement_df["measurement_concept_id"] = 0 # TODO: Update
        #     measurement_df["person_id"] = 0 # TODO: Update
        #     measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
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
            measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

            measurement_df["measurement_concept_id"] = 0 # TODO: Update
            measurement_df["person_id"] = 0 # TODO: Update
            measurement_df["visit_occurrence_id"] = 0 # TODO: Update
            measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "vital_num_value"

        # print(measurement_df.head(1))
        return measurement_df

    def transform_intraop_operation(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_operation..")
        # if len(source_batch) > 0:
        #     measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
        #     measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
        #     measurement_df["measurement_type_concept_id"] = 32879
        #     measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
        #     measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
        #     measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

        #     measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
        #     measurement_df["measurement_concept_id"] = 0 # TODO: Update
        #     measurement_df["person_id"] = 0 # TODO: Update
        #     measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df

    def transform_postop_lab(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_postop_lab..")
        # if len(source_batch) > 0:
        #     measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
        #     measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
        #     measurement_df["measurement_type_concept_id"] = 32879
        #     measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
        #     measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
        #     measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

        #     measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
        #     measurement_df["measurement_concept_id"] = 0 # TODO: Update
        #     measurement_df["person_id"] = 0 # TODO: Update
        #     measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df


    def transform_postop_labsall(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_postop_labsall..")
        # if len(source_batch) > 0:
        #     measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
        #     measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
        #     measurement_df["measurement_type_concept_id"] = 32879
        #     measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
        #     measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
        #     measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

        #     measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
        #     measurement_df["measurement_concept_id"] = 0 # TODO: Update
        #     measurement_df["person_id"] = 0 # TODO: Update
        #     measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df

    def transform_preop_others(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_others..")
        # if len(source_batch) > 0:
        #     measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
        #     measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
        #     measurement_df["measurement_type_concept_id"] = 32879
        #     measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
        #     measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
        #     measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

        #     measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
        #     measurement_df["measurement_concept_id"] = 0 # TODO: Update
        #     measurement_df["person_id"] = 0 # TODO: Update
        #     measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df

    def transform_preop_riskindex(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_riskindex..")
        # if len(source_batch) > 0:
        #     measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
        #     measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
        #     measurement_df["measurement_type_concept_id"] = 32879
        #     measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
        #     measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
        #     measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

        #     measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
        #     measurement_df["measurement_concept_id"] = 0 # TODO: Update
        #     measurement_df["person_id"] = 0 # TODO: Update
        #     measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df

    def transform_intraop_nurvitals(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_nurvitals..")
        # if len(source_batch) > 0:
        #     measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
        #     measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
        #     measurement_df["measurement_type_concept_id"] = 32879
        #     measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
        #     measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
        #     measurement_df["measurement_id"] = range(self.measurement_id_start, self.measurement_id_end + 1)

        #     measurement_df["value_source_value"] = None # TODO: Update Vocab concept id for "preop_lab_result_value"
        #     measurement_df["measurement_concept_id"] = 0 # TODO: Update
        #     measurement_df["person_id"] = 0 # TODO: Update
        #     measurement_df["visit_occurrence_id"] = 0 # TODO: Update

        # print(measurement_df.head(1))
        return measurement_df

    def ingest(self, transformed_batch):
        transformed_batch.to_sql(name="measurement", schema=self.omop_schema, con=self.engine, if_exists="append", index=False)
        print(f"offset {self.offset} limit {self.limit} batch_count {len(transformed_batch)} ingested..")

    def finalize(self):
        # cleanup
        self.engine.dispose()

