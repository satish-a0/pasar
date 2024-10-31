import traceback
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ..db.utils.postgres import postgres
import pandas as pd
# Load environment variables from the .env file
load_dotenv()


class source_to_concept_map:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.source_file = os.path.join(os.getenv("BASE_PATH"), "source_to_concep_map", "v3", "data.csv")
        self.temp_table = f'temp_source_to_concept_map_{os.urandom(15).hex()}'

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
                connection.execute(text(f"Truncate table {self.omop_schema}.source_to_concept_map"))

    def process(self):
        self.retrieve()
        self.ingest()


    def retrieve(self):
        df = pd.read_csv(self.source_file)
        df.drop(["target_vocabulary_id", "invalid_reason"], inplace=True, axis=1)
        # print(df.head(2))
        df.to_sql(name=self.temp_table, schema=self.omop_schema, con=self.engine, if_exists='replace',index=False)

    def ingest(self):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(
                    text(f'SET search_path TO {self.omop_schema}'))
                connection.execute(text(f'''
                                        INSERT INTO source_to_concept_map (
                                                source_code,
                                                source_concept_id,
                                                source_vocabulary_id,
                                                source_code_description,
                                                target_concept_id,
                                                target_vocabulary_id,
                                                invalid_reason,
                                                valid_start_date,
                                                valid_end_date
                                            )
                                                SELECT t.source_code,
                                                    t.source_concept_id,
                                                    t.source_vocabulary_id,
                                                    t.source_code_description,
                                                    CASE
                                                        WHEN t.target_concept_id is null THEN 0
                                                        ELSE t.target_concept_id
                                                    END AS target_concept_id,
                                                    CASE
                                                        WHEN c.vocabulary_id is null THEN ''
                                                        ELSE c.vocabulary_id
                                                    END AS vocabulary_id,
                                                    CASE
                                                        WHEN c.invalid_reason = '' THEN null
                                                        ELSE c.invalid_reason
                                                    END AS invalid_reason,
                                                    CASE
                                                        WHEN c.valid_start_date is null THEN '01-01-1970'
                                                        ELSE c.valid_start_date
                                                    END AS valid_start_date,
                                                    CASE
                                                        WHEN c.valid_end_date is null THEN '12-31-2099'
                                                        ELSE c.valid_end_date
                                                    END AS valid_end_date
                                                FROM { self.temp_table } t
                                                LEFT JOIN concept c ON c.concept_id = t.target_concept_id
                                '''))

    def finalize(self):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f"Drop table {self.omop_schema}.{self.temp_table}"))
        self.engine.dispose()
