import os
from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateSchema, DropSchema
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


class postgres:

    def __init__(self, base_path=None) -> None:
        self.omop_schema = os.getenv("POSTGRES_OMOP_SCHEMA")
        self.omop_db = os.getenv("POSTGRES_DB")
        self.connectable = create_engine(
            f'postgresql+psycopg2://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{self.omop_db}')
        self.base_path = "pypasar/db/sql/postgres" if base_path is None else base_path
        self.files = ["ddl.sql",
                      "primary_keys.sql",
                      "constraints.sql",
                      "drop_constraints.sql", # Certain SQL statements can be commented based on the additional vocabularies like CPT4 are added
                      # "indices.sql"
                      ]

    def create_omop_schema(self) -> None:
        with self.connectable.connect() as connection:
            connection.execute(CreateSchema(
                self.omop_schema, if_not_exists=True))
            connection.commit()
            print(f"{self.omop_schema} created..")

        self.populate_omop_tables()

    def populate_omop_tables(self) -> None:
        with self.connectable.connect() as connection:
            for sql_file in self.files:
                connection.execute(
                    text(f"SET search_path TO {self.omop_schema}"))
                file = open(f"{self.base_path}/{sql_file}")
                escaped_sql = text(file.read())
                connection.execute(escaped_sql)
                connection.commit()
                print(f"Executed {sql_file} for Postgres db {self.omop_db} schema {self.omop_schema}..")

    def drop_omop_schema(self) -> None:
        with self.connectable.connect() as connection:
            connection.execute(DropSchema(
                self.omop_schema, cascade=True, if_exists=True))
            connection.commit()
            print(f"{self.omop_schema} dropped..")

    def get_engine(self):
        return self.connectable
