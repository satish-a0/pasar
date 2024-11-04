import os
import sys
import traceback
from dotenv import load_dotenv
from importlib import import_module
import time
import json
from datetime import timedelta
import logging

from pypasar.db.utils import postgres, final_statistics

# Get LOGLEVEL from env and sets logger log level, defaults to ERROR
logging.basicConfig(level=os.getenv("LOGLEVEL", "ERROR"))

# Load environment variables from the .env file
load_dotenv()

# Ingestion will proceed in the order defined wherein dependencies will be populated first
omop_entities_to_ingest = [
    #'cdm_source',  # Required for OHDSI R Packages like data quality to run
    # 'concept',
    # 'concept_ancestor',
    # 'concept_relationship',
    'source_to_concept_map',
    'care_site',
    'provider',
    'person',
    'observation_period',
    'death',
    'visit_occurrence',
    'visit_detail',
    'condition_occurrence',
    'condition_era',
    'drug_exposure',
    'drug_era',
    'procedure_occurrence',
    'device_exposure',
    'observation',
    'note',
    'specimen',
    'measurement'
]

def select_db_dialect(db_dialect):
    db = None
    match db_dialect:
        case "POSTGRES":
            db = postgres.postgres("pypasar/db/sql/postgres")
            print("Selected POSTGRES Dialect")
        case _:
            print("Db Dialect must be Postgres")
    return db


def db(option):
    db_dialect = os.getenv("DB_DIALECT")
    db = select_db_dialect(db_dialect)
    if db is not None:
        match option:
            case "create_omop_schema":
                db.create_omop_schema()
            case "drop_omop_schema":
                db.drop_omop_schema()
            case _:
                print(
                    "Db argument must be either create_omop_schema or drop_omop_schema")


def etl(tables):
    global omop_entities_to_ingest
    if tables is not None:
        omop_entities_to_ingest = [table.strip() for table in tables.split(',')]
    print(f"OMOP tables to be executed: {omop_entities_to_ingest}")

    table_etl_ingestion_time_dict = {}

    # Start ETL for OMOP Tables
    try:
        for omop_entity in omop_entities_to_ingest:
            print(f"Import {omop_entity}..")
            omop_module = import_module(f'pypasar.omop.{omop_entity}')
            omop_class = getattr(omop_module, omop_entity)()
            print(f"Begin execution for {omop_entity}..")
            start_time = time.monotonic()
            omop_class.execute()
            td = timedelta(seconds=time.monotonic() - start_time)
            table_etl_ingestion_time_dict[omop_entity] = {"time_taken": f"{td.total_seconds()}s"}
            print(f"Completed execution for {omop_entity}: {td.total_seconds()}s")
            print()

        table_count_dict = collect_statistics(omop_entities_to_ingest)
        final_statistic_dict =  {k: v | table_count_dict[k] for k, v in table_etl_ingestion_time_dict.items()}
        print(json.dumps(final_statistic_dict, indent=3))
    except Exception as err:
        raise err

def collect_statistics(omop_entities_to_ingest, printStatistics=False):
    print(f"Begin execution for final_statistics..")
    collect_statistics = final_statistics.final_statistics()
    final_statistic_dict = collect_statistics.execute(omop_entities_to_ingest)
    if printStatistics:
        print(json.dumps(final_statistic_dict, indent=3))
    return final_statistic_dict

# Entrypoint
try:
    entrypoint = sys.argv[1]
    match entrypoint:
        case "db":
            options = None if len(sys.argv) <= 2 else sys.argv[2]
            db(options)
        case "etl":
            tables = None if len(sys.argv) <= 2 else sys.argv[2]
            etl(tables)
        case "stats":
            collect_statistics(omop_entities_to_ingest, True)
        case _:
            print("Entrypoint must be either db / etl / stats")
except Exception as err:
    traceback.print_exc()
