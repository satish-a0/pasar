import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


class PostgresGCP:
    def __init__(self):
        self.db = os.getenv("GCP_POSTGRES_DB")

        # Establish connection to GCP postgres instance
        self.connectable = create_engine(
            f"""postgresql+psycopg2://{os.getenv("GCP_POSTGRES_USER")}:{os.getenv("GCP_POSTGRES_PASSWORD")}@{os.getenv("GCP_POSTGRES_IP")}:5432/{self.db}""")

    def close(self):
        self.connectable.dispose()

    def get_engine(self):
        return self.connectable


# Example code just to test and check if PostgresGCP is able to connect to postgres instance hosted at GCP and query some results
if __name__ == '__main__':
    postgresGcp = PostgresGCP()
    with postgresGcp.get_engine().connect() as connection:
        res = connection.execute(
            text("SELECT * from preop.char LIMIT 5;")).fetchall()
        print(res)
    postgresGcp.close()
