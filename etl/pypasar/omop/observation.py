import os
import time
from sqlalchemy import text
import pandas as pd
from dotenv import load_dotenv

from ..db.utils.postgres import postgres

from .observation_utils.mappings import ObservationMapping
from .observation_utils.config import SOURCE_TABLE_COL_NAME, SOURCE_TABLES, CHUNK_SIZE

import logging
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()

# TODO: FIX CHILD FUNCTION AFFECTING PARENT DF


class observation:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection

    def execute(self):
        try:
            self.initialize()
            self.process()
            self.finalize()
        except Exception as err:
            logger.error(f"Error occurred {self.__class__.__name__}")
            raise err

    def initialize(self):
        # For now always truncate for development
        logger.info("Truncating observation table...")
        with self.engine.connect() as connection:
            with connection.begin():
                # Set schema
                connection.execute(
                    text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))
                # Truncate observation table
                connection.execute(text("Truncate table observation"))
        logger.info("Truncating Done")

    def process(self):
        # Process PASAR to OMOP
        logger.info("Processing PASAR to OMOP...")
        start = time.process_time()
        omop_person_df = self.get_omop_person_table()
        allergy_concepts_df = self.get_allergy_concepts()
        rowsMapped = 0
        with self.engine.connect() as connection:
            for source_table in SOURCE_TABLES:
                for chunk in pd.read_sql(
                    f"SELECT * from {source_table};",
                    con=connection,
                    chunksize=CHUNK_SIZE
                ):
                    mapped_df = self.mapping(
                        chunk, omop_person_df, allergy_concepts_df, source_table, rowsMapped)

                    self.ingest(mapped_df)
                    rowsMapped += len(mapped_df)

        logger.info(
            f"Total Time taken for observation processing: {time.process_time() - start:.3f}s")
        logger.info("Processing Done")

    def mapping(self, df: pd.DataFrame, omop_person_df: pd.DataFrame, allergy_concepts_df: pd.DataFrame, source_table: str, rowsMapped: int) -> pd.DataFrame:
        '''
        # # # NO MAPPING FOR THESE COLUMNS
        # observation_datetime
        # qualifier_concept_id
        # unit_concept_id
        # provider_id
        # visit_detail_id
        # observation_source_concept_id
        # unit_source_value
        # qualifier_source_value
        # observation_event_id
        # obs_event_field_concept_id
        '''

        observation_mapping = ObservationMapping()
        mapped_columns = [
            "person_id",
            "observation_date",
            "visit_occurrence_id",
            "observation_type_concept_id",
            "observation_id",
            "observation_concept_id",
            "observation_source_value",
            "value_source_value",
            "value_as_number",
            "value_as_string"
        ]

        # # # person_id
        res = observation_mapping.map_person_id(df, omop_person_df)
        df = pd.concat([df, res], axis=1)

        # # # observation_date
        res = observation_mapping.map_observation_date(df)
        df = pd.concat([df, res], axis=1)

        # # # visit_occurrence_id
        res = observation_mapping.map_visit_occurrence_id(df)
        # df = pd.concat([df, res], axis=1)

        # # # observation_type_concept_id
        res = observation_mapping.map_observation_type_concept_id(df)
        # df = pd.concat([df, res], axis=1)

        # # # value_as_concept_id
        # value_as_concept is only applicable for preop.char source_table.
        if source_table == "preop.char":
            res = observation_mapping.map_value_as_concept_id(
                df, allergy_concepts_df)
            df = pd.concat([df, res], axis=1)
            mapped_columns.append("value_as_concept_id")

        # # # map_eav will map observation_concept_id, observation_source_value, value_source_value, value_as_number, value_as_string
        df = observation_mapping.map_eav(df, source_table)

        # # # observation_id
        res = observation_mapping.map_observation_id(df, rowsMapped)
        df = pd.concat([df, res], axis=1)

        # Truncate columns that are not omop
        df = df[mapped_columns]

        # # # log random sample from df for sanity check
        logger.info("Sample results")
        logger.info(df.sample(15).sort_index())

        return df

    def ingest(self, df: pd.DataFrame):
        # Ingest into OMOP Table
        logger.info("Ingesting into OMOP Table...")
        start = time.process_time()
        with self.engine.connect() as connection:
            df.to_sql(name='observation',
                      con=connection, if_exists='append', schema=os.getenv("POSTGRES_OMOP_SCHEMA"), index=False)
        logger.info(
            f"Total Time taken for observation ingestion: {time.process_time() - start:.3f}s")
        logger.info("Ingestion Done")

    def finalize(self):
        self.engine.dispose()

    def get_data(self) -> pd.DataFrame:
        with self.engine.connect() as connection:
            df = pd.DataFrame()
            for source in SOURCE_TABLES:
                temp = pd.read_sql(
                    f"SELECT * from {source} LIMIT 1000;",
                    con=connection
                )
                # Add a column to indicate which source row is from
                temp[SOURCE_TABLE_COL_NAME] = source
                df = pd.concat([df, temp], ignore_index=True)
        return df

    def get_omop_person_table(self) -> pd.DataFrame:
        with self.engine.connect() as connection:
            df = pd.read_sql(
                text(
                    f"""SELECT person_id, person_source_value from {os.getenv("POSTGRES_OMOP_SCHEMA")}.person;"""
                ),
                con=connection
            )
            return df

    def get_allergy_concepts(self) -> pd.DataFrame:
        with self.engine.connect() as connection:
            df = pd.read_sql(
                text(
                    f"""select concept_id, concept_name from {os.getenv("POSTGRES_OMOP_SCHEMA")}.concept where concept_name like 'Allergy to %';"""
                ),
                con=connection
            )
            return df
