import psycopg2
import os

# from dotenv import load_dotenv

# load_dotenv()
# host = os.environ.get("postgres_host")
# user = os.environ.get("postgres_user")
# password = os.environ.get("postgres_pass")
# database = os.environ.get("postgres_db")


# def get_connection():
#     with psycopg2.connect(
#         host=host,
#         user=user,
#         password=password,
#         database=database,
# )

def get_connection(creds):
    return psycopg2.connect(
        f"dbname={creds['db']} user={creds['user']} password={creds['password']} host={creds['host']} port={creds['port']}")
        # with conn.cursor() as cur:
        #     for statement in statements:
        #         cur.execute(statement)
        #     conn.commit()