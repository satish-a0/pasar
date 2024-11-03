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
        self.temp_concept_table = f'temp_concept_measurement_{os.urandom(15).hex()}'
        self.measurement_id_start = 1
        self.measurement_aimsvitals_fetch_limit = int(os.getenv("OMOP_MEASUREMENT_INTRAOP_AIMSVITALS_FETCH_LIMIT", 0))
        self.source_tables_cols = [{"table": self.source.PREOP_LAB.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "preop_lab_test_description": str,
                                              "preop_lab_result_value": float, 
                                              "preop_lab_collection_datetime": "datetime64[ns]", "person_id": int, "visit_occurrence_id": int}},
                                   {"table": self.source.PREOP_CHAR.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "height": float, "weight": float, "bmi": float, "systolic_bp": float,
                                              "diastolic_bp": float, "heart_rate": float,
                                              "o2_saturation": float, "o2_supplementaries": str,
                                              "temperature": float, "pain_score": float, "person_id": int, "visit_occurrence_id": int}}, 
                                   {"table": self.source.INTRAOP_OPERATION.value, 
                                    "columns": {"anon_case_no": str, "vital_code": str,
                                              "vital_signs_result": float, "vital_signs_taken_datetime": "datetime64[ns]",
                                              "vital_signs_taken_date": "datetime64[ns]", 
                                              "vital_signs_taken_time": str, "person_id": int, "visit_occurrence_id": int}}, 
                                   {"table": self.source.POSTOP_LAB.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, 
                                              "postop_lab_collection_datetime_max": "datetime64[ns]",
                                              "postop_lab_collection_datetime_min": "datetime64[ns]", 
                                              "postop_lab_test_desc": str, "person_id": int, "visit_occurrence_id": int,
                                              "postop_result_value_max": float, "postop_result_value_min": float}}, 
                                   {"table": self.source.POSTOP_LABSALL.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "gen_lab_lab_test_code": str,
                                              "gen_lab_result_value": str, "gen_lab_specimen_collection_date": "datetime64[ns]",
                                              "gen_lab_specimen_collection_time": str, "person_id": int, "visit_occurrence_id": int}},
                                   {"table": self.source.PREOP_OTHERS.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "asa_score_aims": float, "asa_score_eaf": str,
                                              "efs_total_score": int, "person_id": int, "visit_occurrence_id": int}}, 
                                   {"table": self.source.PREOP_RISKINDEX.value, 
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "session_startdate": "datetime64[ns]",
                                              "asa_class": str, "cri_functional_status": str,
                                              "cardiac_risk_index": float, "cardiac_risk_class": str,
                                              "osa_risk_index": str, "act_risk": str, "person_id": int, "visit_occurrence_id": int}}, 
                                   {"table": self.source.INTRAOP_NURVITALS.value, 
                                    "columns": {"anon_case_no": str, "id": int, "person_id": int,
                                              "authored_datetime": "datetime64[ns]", "document_item_name": str, "value_text": str}},
                                   {"table": self.source.INTRAOP_AIMSVITALS.value,  # 11 million records to be ingested at the end
                                    "columns": {"anon_case_no": str, "id": int,
                                              "session_id": int, "vitalcode": str,
                                              "vital_num_value": float, "vitaldt": "datetime64[ns]", 
                                              "vital_date": "datetime64[ns]", "vital_time": str, "person_id": int, "visit_occurrence_id": int}} ]

    def execute(self):
        try:
            self.initialize()
            self.process()
            self.finalize()
        except Exception as err:
            print(f"Error occurred {self.__class__.__name__}")
            raise err

    def initialize(self):
        #Truncate
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f"Truncate table {self.omop_schema}.measurement CASCADE"))
        # Create temporary concept table
        self.create_temp_concept_table()


    def create_temp_concept_table(self):
         with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f'SET search_path TO {self.omop_schema}'))
                # This is used to map source to concept map table
                connection.execute(text(f'''CREATE TEMPORARY TABLE { self.temp_concept_table } AS
                                            select sc.source_code, sc.target_concept_id, sc.source_vocabulary_id
                                            from {self.omop_schema}.source_to_concept_map sc
                                                inner join {self.omop_schema}.concept c
                                                    ON sc.target_vocabulary_id = c.vocabulary_id
                                                    and c.domain_id = 'Measurement'
                                                    and c.invalid_reason is null
                                                    and sc.target_concept_id = c.concept_id'''
                                    )
                                )

    def process(self):
        for source_table_cols in self.source_tables_cols:
            # if source_table_cols["table"] not in [
            #                                     #   f"{self.source_preop_schema}.lab", 
            #                                     #   f"{self.source_preop_schema}.char", 
            #                                        f"{self.source_intraop_schema}.aimsvitals"
            #                                     ]:
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
            # print(f"measurement id start: {self.measurement_id_start}")
            transformed_batch = self.transform(source_table_cols, source_batch)
            self.measurement_id_start += len(transformed_batch)
            del source_batch
            self.ingest(transformed_batch)
            del transformed_batch
            gc.collect()
            self.offset = self.offset + self.limit
            # break


    def fetch_total_count_source_table(self, source_table_name):
        source_total_table_count = 0 
        with self.engine.connect() as connection:
            with connection.begin():
                res = connection.execute(text(f"select count(1) from {source_table_name}"))
                source_total_table_count = res.first()[0]

        # This is a special case to control how much source data is ingested from intraop aimsvitals table since it takes a few days to complete
        if source_table_name == self.source.INTRAOP_AIMSVITALS.value:
            if self.measurement_aimsvitals_fetch_limit > 0:
                source_total_table_count = self.measurement_aimsvitals_fetch_limit # Overrides the actual total count

        return source_total_table_count

    def retrieve(self, source_table_cols):
        source_batch = self.fetch_in_batch_source_table(source_table_cols)
        source_df = pd.DataFrame(source_batch.fetchall())
        source_df.columns = source_table_cols["columns"].keys()
        # print(source_df.head(1))
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
        
        # Inner join with OMOP Person table
        select_sql += f''' FROM {source_table_cols['table']}
                           INNER JOIN {self.omop_schema}.person ON anon_case_no = person_source_value'''

        # Inner join with OMOP Visit_occurrence table
        if "visit_occurrence_id" in source_columns:
            select_sql += f''' INNER JOIN (
                                            SELECT visit_occurrence_id,
                                                row_number() over (
                                                    partition by CAST(
                                                        LEFT(
                                                            CAST(visit_occurrence_id AS TEXT),
                                                            LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2
                                                        ) AS INTEGER
                                                    )
                                                ) rownum,
                                                CAST(
                                                    LEFT(
                                                        CAST(visit_occurrence_id AS TEXT),
                                                        LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2
                                                    ) AS INTEGER
                                                ) AS truncated_visit_occurrence_id
                                            FROM VISIT_OCCURRENCE
                                        ) v ON v.truncated_visit_occurrence_id = session_id
                                        AND v.rownum = 1'''
        
        select_sql += f" order by anon_case_no LIMIT {self.limit} OFFSET {self.offset}"
        # select_sql += f" order by anon_case_no LIMIT 2"
        # print(select_sql)
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
        
        # Load specific source codes mapping into df
        concept_df = pd.read_sql_query(f"select source_code, target_concept_id from {self.temp_concept_table} where source_vocabulary_id='SG_PASAR_PREOP_LAB'", con=self.engine)

        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["preop_lab_collection_datetime"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["preop_lab_collection_datetime"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["preop_lab_result_value"]
            measurement_df["measurement_source_value"] = source_batch["preop_lab_test_description"]
            
            # Left Join with temporary concept df
            measurement_df = pd.merge(measurement_df, concept_df, how="left", left_on='measurement_source_value', right_on='source_code')
            measurement_df["measurement_concept_id"] = measurement_df["target_concept_id"]
            measurement_df.fillna({"measurement_concept_id": 0}, inplace=True) # For those missing standard concept mapping
            measurement_df.drop(['source_code', 'target_concept_id'], axis=1, inplace=True)
            
            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))


        # print(measurement_df.head(3))
        del concept_df
        return measurement_df

    def transform_preop_char(self, source_table_cols, source_batch, measurement_df):
        # 1 to Many
        print(f"INSIDE transform_preop_char..")
        
        measurement_score_columns = ["height","weight","bmi", "systolic_bp", "diastolic_bp", "heart_rate", "o2_saturation", "temperature", "pain_score"]
        # 1:1 mapping index between measurement_score_columns and measurement_char_concept_ids
        measurement_char_concept_ids = [3036277, 3025315, 3038553, 3004249, 3012888, 3027018, 3013502, 3020891, 43055141]
        
        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["session_startdate"]).dt.date
            measurement_df["measurement_type_concept_id"] = 32879
            value_as_number_df = source_batch[measurement_score_columns] # Assumption Ignoring "o2_supplementaries" since its an additional value for the o2_saturation
            measurement_df["value_as_number"] = value_as_number_df.apply(lambda row: row.tolist(), axis=1)
            measurement_df["measurement_source_value"] = [measurement_score_columns]*len(measurement_df)
            # value_as_source_df = source_batch[["o2_supplementaries"]]
            # measurement_df["value_source_value"] = value_as_source_df.apply(lambda row: row.tolist(), axis=1)
            measurement_df["measurement_concept_id"] = [measurement_char_concept_ids]*len(measurement_df)

            # To maintain linkage between the records defined in measurement_score_columns
            measurement_df["meas_event_field_concept_id"] = 1147330
            measurement_df["unique_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            measurement_df["measurement_event_id"] = measurement_df["unique_id"].apply(lambda row: [row]*len(measurement_score_columns))
            del measurement_df["unique_id"]

            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"] 

            # Transpose magic happens
            measurement_df = measurement_df.explode(['value_as_number', 'measurement_source_value', 'measurement_concept_id', 'measurement_event_id'])
            measurement_df = measurement_df.dropna(subset=['value_as_number'], thresh=1)
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            # print(measurement_df.head(3))

        return measurement_df

    def transform_intraop_aimsvitals(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_aimsvitals..")
        
        # Load specific source codes mapping into df
        concept_df = pd.read_sql_query(f"select source_code, target_concept_id from {self.temp_concept_table} where source_vocabulary_id='SG_PASAR_INTRAOP_AIMS_VITALS'", con=self.engine)

        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["vital_date"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["vitaldt"]
            measurement_df["measurement_time"] = source_batch["vital_time"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["vital_num_value"]
            measurement_df["measurement_source_value"] = source_batch["vitalcode"]
            
            # Left Join with temporary concept df
            measurement_df = pd.merge(measurement_df, concept_df, how="left", left_on='measurement_source_value', right_on='source_code')
            measurement_df["measurement_concept_id"] = measurement_df["target_concept_id"]
            measurement_df.fillna({"measurement_concept_id": 0}, inplace=True) # For those missing standard concept mapping
            measurement_df.drop(['source_code', 'target_concept_id'], axis=1, inplace=True)
            
            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

        # print(measurement_df.head(1))
        return measurement_df

    def transform_intraop_operation(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_operation..")

        # Load specific source codes mapping into df
        concept_df = pd.read_sql_query(f"select source_code, target_concept_id from {self.temp_concept_table} where source_vocabulary_id='SG_PASAR_INTRAOP_AIMS_VITALS'", con=self.engine)

        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["vital_signs_taken_date"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["vital_signs_taken_datetime"]
            measurement_df["measurement_time"] = source_batch["vital_signs_taken_time"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["vital_signs_result"]
            measurement_df["measurement_source_value"] = source_batch["vital_code"]

            # Left Join with temporary concept df
            measurement_df = pd.merge(measurement_df, concept_df, how="left", left_on='measurement_source_value', right_on='source_code')
            measurement_df["measurement_concept_id"] = measurement_df["target_concept_id"]
            measurement_df.fillna({"measurement_concept_id": 0}, inplace=True) # For those missing standard concept mapping
            measurement_df.drop(['source_code', 'target_concept_id'], axis=1, inplace=True)            
            
            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

        # print(measurement_df.head(1))
        del concept_df
        return measurement_df

    def transform_postop_lab(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_postop_lab..")

        # Load specific source codes mapping into df
        concept_df = pd.read_sql_query(f"select source_code, target_concept_id from {self.temp_concept_table} where source_vocabulary_id='SG_PASAR_POSTOP_LABS_ALL'", con=self.engine)

        # Assumption picking only max values for simplicity and ignoring the min values
        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["postop_lab_collection_datetime_max"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["postop_lab_collection_datetime_max"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_as_number"] = source_batch["postop_result_value_max"]
            measurement_df["measurement_source_value"] = source_batch["postop_lab_test_desc"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

            # Left Join with temporary concept df
            measurement_df = pd.merge(measurement_df, concept_df, how="left", left_on='measurement_source_value', right_on='source_code')
            measurement_df["measurement_concept_id"] = measurement_df["target_concept_id"]
            measurement_df.fillna({"measurement_concept_id": 0}, inplace=True) # For those missing standard concept mapping
            measurement_df.drop(['source_code', 'target_concept_id'], axis=1, inplace=True)         

            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

        # print(measurement_df.head(1))
        del concept_df
        return measurement_df


    def transform_postop_labsall(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_postop_labsall..")

        # Load specific source codes mapping into df
        concept_df = pd.read_sql_query(f"select source_code, target_concept_id from {self.temp_concept_table} where source_vocabulary_id='SG_PASAR_POSTOP_LABS_ALL'", con=self.engine)

        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["gen_lab_specimen_collection_date"]).dt.date
            measurement_df["measurement_datetime"] = None
            measurement_df["measurement_time"] = source_batch["gen_lab_specimen_collection_time"]
            measurement_df["measurement_type_concept_id"] = 32879
            measurement_df["value_source_value"] = source_batch["gen_lab_result_value"]
            measurement_df["measurement_source_value"] = source_batch["gen_lab_lab_test_code"]

            # Left Join with temporary concept df
            measurement_df = pd.merge(measurement_df, concept_df, how="left", left_on='measurement_source_value', right_on='source_code')
            measurement_df["measurement_concept_id"] = measurement_df["target_concept_id"]
            measurement_df.fillna({"measurement_concept_id": 0}, inplace=True) # For those missing standard concept mapping
            measurement_df.drop(['source_code', 'target_concept_id'], axis=1, inplace=True)
            
            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"]
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

        # print(measurement_df.head(1))
        del concept_df
        return measurement_df

    def transform_preop_others(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_others..")
        # 1 to Many
        measurement_score_columns = ["efs_total_score","asa_score_aims","asa_score_eaf"]
        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["session_startdate"]).dt.date
            measurement_df["measurement_type_concept_id"] = 32879
            value_as_number_df = source_batch[measurement_score_columns]
            measurement_df["value_as_number"] = value_as_number_df.apply(lambda row: row.tolist(), axis=1)
            measurement_df["measurement_source_value"] = [measurement_score_columns]*len(measurement_df)
            measurement_df["measurement_concept_id"] = 0 # Lack of information on mapping
            
            # To maintain linkage between the records defined in measurement_score_columns
            measurement_df["meas_event_field_concept_id"] = 1147330
            measurement_df["unique_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            measurement_df["measurement_event_id"] = measurement_df["unique_id"].apply(lambda row: [row]*len(measurement_score_columns))
            del measurement_df["unique_id"]

            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"]
            
            measurement_df = measurement_df.explode(['value_as_number', 'measurement_source_value', 'measurement_event_id'])
            measurement_df = measurement_df.dropna(subset=['value_as_number'], thresh=1)
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            # print(measurement_df.head(len(measurement_df)))
        return measurement_df

    def transform_preop_riskindex(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_preop_riskindex..")
        # 1 to Many
        # Assumption adding cardiac_risk_index as part of value_source_value instead of value_as_number for simplicity
        measurement_score_columns = ["asa_class","cri_functional_status","cardiac_risk_index","cardiac_risk_class",         "osa_risk_index","act_risk"]
        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["session_startdate"]).dt.date
            measurement_df["measurement_type_concept_id"] = 32879
            value_as_source_df = source_batch[measurement_score_columns]
            measurement_df["value_source_value"] = value_as_source_df.apply(lambda row: row.tolist(), axis=1)
            measurement_df["measurement_source_value"] = [measurement_score_columns]*len(measurement_df)
            measurement_df["measurement_concept_id"] = 0 # Lack of information on mapping

            # To maintain linkage between the records defined in measurement_score_columns
            measurement_df["meas_event_field_concept_id"] = 1147330
            measurement_df["unique_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            measurement_df["measurement_event_id"] = measurement_df["unique_id"].apply(lambda row: [row]*len(measurement_score_columns))
            del measurement_df["unique_id"]

            measurement_df["visit_occurrence_id"] = source_batch["visit_occurrence_id"] 
            
            measurement_df = measurement_df.explode(['value_source_value', 'measurement_source_value', 'measurement_event_id'])
            measurement_df = measurement_df.dropna(subset=['value_source_value'], thresh=1)
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))
            # print(measurement_df.head(len(measurement_df)))
        return measurement_df

    def transform_intraop_nurvitals(self, source_table_cols, source_batch, measurement_df):
        print(f"INSIDE transform_intraop_nurvitals..")

        # Load specific source codes mapping into df
        concept_df = pd.read_sql_query(f"select source_code, target_concept_id from {self.temp_concept_table} where source_vocabulary_id='SG_PASAR_INTRAOP_NUR_VITALS'", con=self.engine)

        if len(source_batch) > 0:
            measurement_df["person_id"] = source_batch["person_id"]
            measurement_df["measurement_date"] = pd.to_datetime(source_batch["authored_datetime"]).dt.date
            measurement_df["measurement_datetime"] = source_batch["authored_datetime"]
            measurement_df["measurement_source_value"] = source_batch["document_item_name"]
            measurement_df["measurement_type_concept_id"] = 32879

            measurement_df["value_source_value"] = source_batch["value_text"] # Contains mix of text and numeric. Could replicate the numeric values to othe column value_as_number based on the type of document_item_name/measurement_source_value
            
            # Left Join with temporary concept df
            measurement_df = pd.merge(measurement_df, concept_df, how="left", left_on='measurement_source_value', right_on='source_code')
            measurement_df["measurement_concept_id"] = measurement_df["target_concept_id"]
            measurement_df.fillna({"measurement_concept_id": 0}, inplace=True) # For those missing standard concept mapping
            measurement_df.drop(['source_code', 'target_concept_id'], axis=1, inplace=True)
            
            measurement_df["visit_occurrence_id"] = None # There's no session_id involved with source table
            measurement_df["measurement_id"] = range(self.measurement_id_start, (self.measurement_id_start + len(measurement_df)))

        # print(measurement_df.head(1))
        del concept_df
        return measurement_df

    def ingest(self, transformed_batch):
        transformed_batch.to_sql(name="measurement", schema=self.omop_schema, con=self.engine, if_exists="append", index=False)
        print(f"offset {self.offset} limit {self.limit} batch_count {len(transformed_batch)} ingested..")

    def finalize(self):
        # cleanup
        self.engine.dispose()

