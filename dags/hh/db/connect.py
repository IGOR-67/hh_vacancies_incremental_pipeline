import os
from dotenv import load_dotenv
import logging
import psycopg2


load_dotenv()

DB_HOST = os.getenv("DB_HOST", "host.docker.internal")
DB_NAME = os.getenv("DB_NAME", "hh_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5400))

# Параметры подключения к PostgreSQL
PG_DSN = f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}"

def get_conn():
    logging.info(
        "Connecting to PostgreSQL: "
        f"host={DB_HOST}, "
        f"port={DB_PORT}, "
        f"db={DB_NAME}, "
        f"user={DB_USER}"
    )
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    return conn