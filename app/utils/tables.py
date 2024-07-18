from sqlalchemy import create_engine, text
from contextlib import contextmanager
import sqlalchemy.exc

# Global engine configuration
engine = create_engine('postgresql+psycopg2://user:password@postgres:5432/mydatabase', pool_size=10, max_overflow=20)

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
            ReportingStartDate DATE,
            ReportedMeasureCode VARCHAR,
            DataSetId INT PRIMARY KEY,
            MeasureCode VARCHAR,
            DatasetName TEXT
        );""",
        """CREATE TABLE hospitals (
            Code VARCHAR(255) PRIMARY KEY,
            Name TEXT,
            Type TEXT,
            Latitude FLOAT,
            Longitude FLOAT,
            Sector TEXT,
            Open_Closed TEXT,
            State TEXT,
            LHN TEXT,
            PHN TEXT
        );""",
        """CREATE TABLE measurements (
            MeasureCode VARCHAR PRIMARY KEY,
            MeasureName TEXT
        );""",
        """CREATE TABLE reported_measurements (
            ReportedMeasureCode VARCHAR PRIMARY KEY,
            ReportedMeasureName TEXT
        );""",
        """CREATE TABLE values (
            DatasetId VARCHAR,
            ReportingUnitCode VARCHAR,
            Value FLOAT,
            Caveats TEXT
        );"""
    ]

    for sql in tables_sql:
        create_table(sql)
