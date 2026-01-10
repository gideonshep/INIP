import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

def conn_str() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "inip")
    user = os.getenv("DB_USER", "inip")
    pwd  = os.getenv("DB_PASSWORD", "inip_password")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"

def get_conn():
    return psycopg.connect(conn_str())
