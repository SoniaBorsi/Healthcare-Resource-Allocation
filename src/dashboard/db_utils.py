# db_utils.py

import time
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import OperationalError
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set the log level to INFO or DEBUG as needed

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Set the handler log level

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

# Database Connection Details
POSTGRES_CONNECTION = {
    "drivername": "postgresql",
    "host": "postgres",        # Use 'db' if that's the name of your service in docker-compose
    "port": "5432",
    "username": "myuser",
    "password": "mypassword",
    "database": "mydatabase"
}

def get_engine_with_retry(retries=5, delay=2):
    """
    Creates a SQLAlchemy engine with retries and exponential backoff.
    """
    attempt = 0
    current_delay = delay
    while attempt < retries:
        try:
            engine = create_engine(URL.create(**POSTGRES_CONNECTION))
            # Test the connection
            with engine.connect() as conn:
                logger.info("Database connection established.")
            return engine
        except OperationalError as e:
            attempt += 1
            logger.warning(f"Database connection failed (attempt {attempt}/{retries}): {e}")
            time.sleep(current_delay)
            current_delay *= 2  # Exponential backoff
    logger.error("Could not connect to the database after several attempts.")
    raise Exception("Could not connect to the database after several attempts.")

# Initialize the engine
engine = get_engine_with_retry()

def fetch_data(sql, params=None):
    """
    Executes a SQL query and returns the result as a pandas DataFrame.
    """
    try:
        with engine.connect() as conn:
            if params:
                df = pd.read_sql_query(sql, conn, params=params)
            else:
                df = pd.read_sql_query(sql, conn)
        logger.debug(f"Query executed successfully: {sql}")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data for query: {sql} - Error: {e}")
        return pd.DataFrame()

from sqlalchemy import text

def check_data_ready(table_name, min_rows=1):
    """
    Checks if a table exists and has at least min_rows rows.
    Returns True if data is ready, False otherwise.
    """
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            if table_name in inspector.get_table_names():
                # Use the text() function to wrap the query
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                if count >= min_rows:
                    logger.info(f"Table '{table_name}' is ready with {count} rows.")
                    return True
                else:
                    logger.warning(f"Table '{table_name}' exists but has insufficient rows ({count} < {min_rows}).")
                    return False
            else:
                logger.warning(f"Table '{table_name}' does not exist.")
                return False
    except Exception as e:
        logger.error(f"Failed to check data readiness for table '{table_name}': {e}")
        return False

