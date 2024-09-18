import os
from sqlalchemy import text
import pandas as pd
from dotenv import load_dotenv

from ..db.utils.postgres import postgres

from .observation_utils.mappings import map_observation_id, map_observation_date, map_value_as_string
from .observation_utils.config import SOURCE_TABLE_COL_NAME, SOURCE_TABLES

import logging
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()


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
        with self.engine.connect() as connection:
            with connection.begin():
                # Set schema
                connection.execute(
                    text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))
                # Insert record
                connection.execute(text("Truncate table observation"))

    def process(self):
        # In batches
        # Read from source
        df = self.get_data()

        # Transform PASAR to OMOP
        # Initialize empty dataframe to continously append mapped columns
        mapped_df = pd.DataFrame()

        res = map_observation_id(df)
        mapped_df = pd.concat([mapped_df, res], axis=1)

        res = map_observation_date(df)
        mapped_df = pd.concat([mapped_df, res], axis=1)

        res = map_value_as_string(df)
        mapped_df = pd.concat([mapped_df, res], axis=1)

        # log random sample from final mapped_df for sanity check
        logger.info(mapped_df.sample(15).sort_index())

        # Ingest into OMOP Table
        # TODO: Add insertion of mapped_df into OMOP table

        pass

    def finalize(self):
        # Verify if needed
        pass

    def get_data(self) -> pd.DataFrame:
        with self.engine.connect() as connection:
            df = pd.DataFrame()
            for source in SOURCE_TABLES:
                temp = pd.read_sql(
                    f"SELECT * from {source} LIMIT 10;",
                    con=connection
                )
                # Add a column to indicate which source row is from
                temp[SOURCE_TABLE_COL_NAME] = source
                df = pd.concat([df, temp], ignore_index=True)

        return df
