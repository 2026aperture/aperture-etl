import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


def get_connection():
    """
    Create a Postgres connection using environment variables from .env.

    Required:
    - DB_HOST
    - DB_PORT
    - DB_NAME
    - DB_USER
    - DB_PASSWORD
    """
    load_dotenv()

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        cursor_factory=RealDictCursor
    )
