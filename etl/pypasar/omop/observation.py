import os
import time
from sqlalchemy import text
import pandas as pd
from dotenv import load_dotenv

from ..db.utils.postgres import postgres

from .observation_utils.mappings import ObservationMappings
from .observation_utils.config import SOURCE_TABLE_COL_NAME, SOURCE_TABLES, ObservationMappingConfig

import logging
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()


class observation:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection
        # Initialize empty dataframe to continously append mapped columns
        self.mapped_df = pd.DataFrame()

    def execute(self):
        try:
            self.initialize()
            self.process()
            self.ingest()
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
        # In batches
        # Read from source
        df = self.get_data()

        # # # observation_id
        res = ObservationMappings.map_observation_id(df)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # person_id
        # TODO: Add logic
        # Temp add dummy values to test insertion into destination schema
        self.mapped_df["person_id"] = range(1, len(self.mapped_df) + 1)

        # # # observation_concept_id
        # TODO: Add logic
        # Temp add dummy values to test insertion into destination schema
        self.mapped_df["observation_concept_id"] = range(
            1, len(self.mapped_df) + 1)

        # # # observation_date
        res = ObservationMappings.map_observation_date(df)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # observation_datetime
        # NO MAPPING

        # # # observation_type_concept_id
        res = ObservationMappings.map_observation_type_concept_id(df)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # value_as_number
        res = ObservationMappings.map_value_as_number(df)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # value_as_string
        res = ObservationMappings.concatenate_multiple_columns_into_one(
            df, ObservationMappingConfig.value_as_string_mapping)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # value_as_concept_id
        allergy_concepts_df = self.get_allergy_concepts()
        res = ObservationMappings.map_value_as_concept_id(
            df, allergy_concepts_df)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # qualifier_concept_id
        # NO MAPPING

        # # # unit_concept_id
        # NO MAPPING

        # # # provider_id
        # NO MAPPING

        # # # visit_occurrence_id
        res = ObservationMappings.map_visit_occurrence_id(df)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # visit_detail_id
        # NO MAPPING

        # # # observation_source_value
        res = ObservationMappings.concatenate_multiple_columns_into_one(
            df, ObservationMappingConfig.observation_source_value_mapping)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # observation_source_concept_id
        # NO MAPPING

        # # # unit_source_value
        # NO MAPPING

        # # # qualifier_source_value
        # NO MAPPING

        # # # value_source_value
        res = ObservationMappings.concatenate_multiple_columns_into_one(
            df, ObservationMappingConfig.value_source_value_mapping)
        self.mapped_df = pd.concat([self.mapped_df, res], axis=1)

        # # # observation_event_id
        # NO MAPPING

        # # # obs_event_field_concept_id
        # NO MAPPING

        # # # log random sample from final self.mapped_df for sanity check
        logger.info("Final results")
        logger.info(self.mapped_df.sample(15).sort_index())

        logger.info(
            f"Total Time taken for observation processing: {time.process_time() - start:.3f}s")
        logger.info("Processing Done")

    def ingest(self):
        # Ingest into OMOP Table
        logger.info("Ingesting into OMOP Table...")
        with self.engine.connect() as connection:
            self.mapped_df.to_sql(name='observation',
                                  con=connection, if_exists='append', schema=os.getenv("POSTGRES_OMOP_SCHEMA"), index=False)
        logger.info("Ingestion Done")

    def finalize(self):
        # Verify if needed
        pass

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

    def get_allergy_concepts(self) -> pd.DataFrame:
        with self.engine.connect() as connection:
            connection.execute(
                text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))
            df = pd.read_sql(
                text(
                    f"""select concept_id, concept_name from {os.getenv("POSTGRES_OMOP_SCHEMA")}.concept where concept_name like 'Allergy to %';"""
                ),
                con=connection
            )
            return df
