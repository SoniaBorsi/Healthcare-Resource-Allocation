from sqlalchemy import create_engine, text
from contextlib import contextmanager
import sqlalchemy.exc

# Global engine configuration
engine = create_engine('postgresql+psycopg2://myuser:mypassword@postgres:5432/mydatabase', pool_size=10, max_overflow=20)

@contextmanager
def get_connection():
    """Provide a transactional scope around a series of operations."""
    connection = engine.connect()
    transaction = connection.begin()
    try:
        yield connection
        transaction.commit()
    except sqlalchemy.exc.SQLAlchemyError as e:
        print("An error occurred:", e)
        transaction.rollback()
    finally:
        connection.close()

def create_table(sql_command):
    """Create a table in the PostgreSQL database."""
    try:
        with get_connection() as conn:
            conn.execute(text(sql_command))
            print("Table created successfully.")
    except Exception as e:
        print("Failed to create table:", e)

def schema():
    """Create the schema for the database by initializing required tables."""
    tables_sql = [
        """CREATE TABLE IF NOT EXISTS datasets (
            reportingstartdate DATE,
            reportedmeasurecode VARCHAR,
            datasetid INT PRIMARY KEY,
            measurecode VARCHAR,
            datasetname TEXT,
            stored BOOLEAN DEFAULT FALSE
        );""",
        """CREATE TABLE IF NOT EXISTS hospitals (
            code VARCHAR(255) PRIMARY KEY,
            name TEXT,
            type TEXT,
            latitude FLOAT,
            longitude FLOAT,
            sector TEXT,
            open_closed TEXT,
            state TEXT,
            lhn TEXT,
            phn TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS measurements (
            measurecode VARCHAR PRIMARY KEY,
            measurename TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS reported_measurements (
            reportedmeasurecode VARCHAR PRIMARY KEY,
            reportedmeasurename TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS info (
            datasetid INT,
            reportingunitcode VARCHAR,
            value FLOAT,
            caveats TEXT,
            id VARCHAR PRIMARY KEY
        );"""
    ]

    for sql in tables_sql:
        create_table(sql)
