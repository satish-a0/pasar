import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import sshtunnel

# Load environment variables from the .env file
load_dotenv()


class PostgresGCP:
    def __init__(self):
        self.db = os.getenv("GCP_POSTGRES_DB")

        # Start ssh tunnel 
        local_tunnel_port = int(os.getenv("LOCAL_TUNNEL_PORT"))
        server = sshtunnel.open_tunnel(
            (os.getenv("GCP_VM_IP"), 22),
            ssh_username=os.getenv("GCP_VM_USERNAME"),
            ssh_pkey=f"{os.getenv("GCP_VM_PKEY_PATH")}",
            remote_bind_address=(os.getenv("GCP_POSTGRES_IP"), 5432),
            local_bind_address=('0.0.0.0', local_tunnel_port)
        )
        server.start()
        self.server = server 

        # Establish connection to google cloud postgres instance via binded port from ssh tunnel
        self.connectable = create_engine(f"postgresql+psycopg2://{os.getenv("GCP_POSTGRES_USER")}:{os.getenv("GCP_POSTGRES_PASSWORD")}@localhost:{local_tunnel_port}/{self.db}")

    def close_tunnel(self):
        self.server.close()

    def get_engine(self):
        return self.connectable


# Example code just to test and check if PostgresGCP is able to connect to postgres instance hosted at google cloud via ssh tunnel to google cloud vm and query results
if __name__ == '__main__':
    postgresGcp = PostgresGCP()
    with postgresGcp.get_engine().connect() as connection:
        res = connection.execute(text("SELECT * from preop.char LIMIT 5;")).fetchall()
        print(res)
    postgresGcp.close_tunnel()