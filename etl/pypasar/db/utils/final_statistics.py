import traceback
import os
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
# Load environment variables from the .env file
from .postgres import postgres
load_dotenv()


class final_statistics:

    def __init__(self):
        self.engine = postgres().get_engine()  # Get PG Connection

    def execute(self, omop_entities):
        try:
            table_dict = self.process(omop_entities)
            self.finalize()
            return table_dict
        except Exception as err:
            print(f"Error occurred {self.__class__.__name__}")
            raise err

    def process(self, omop_entities):
        unionSql = ""
        for idx, entity in enumerate(omop_entities):
            if idx < len(omop_entities) - 1:
                unionSql += f"""SELECT '{entity}' as table_name, count(1) as table_count 
                                    FROM {entity} UNION """
            else:
                unionSql += f"""SELECT '{entity}' as table_name, count(1) as table_count FROM {entity}
                                ORDER BY table_name"""

        # print(unionSql)
        with self.engine.connect() as connection:
            with connection.begin():
                # Set schema
                connection.execute(
                    text(f'SET search_path TO {os.getenv("POSTGRES_OMOP_SCHEMA")}'))
                # Select count, name for all tables
                res = connection.execute(text(unionSql))
                rows = res.fetchall()
                # print(rows)
                table_dict = {}
                total_rows = 0
                for row in rows:
                    total_rows += int(row[1])
                    table_dict[row[0]] = {"records_count": row[1]}
                table_dict["total"] = {"records_count": total_rows}
                # print(table_dict)
                return table_dict


    
    def finalize(self):
        # cleanup
        self.engine.dispose()