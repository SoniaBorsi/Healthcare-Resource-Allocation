import requests
import tempfile
import logging
import pika
import psycopg2
from io import StringIO
from pyspark.sql import SparkSession
from data_collector.pipeline import consume_from_rabbitmq, send_to_rabbitmq, callback

def download_datasets_csv():
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  # Replace with your actual access token
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            with open('datasets.csv', 'wb') as f:  # Open file in binary mode
                f.write(response.content)
            print("CSV file downloaded successfully and saved as 'datasets.csv'.")
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Exception occurred while fetching datasets list: {e}")

def insert_into_postgresql(data_frame):
    conn_params = {
        'dbname': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': 5432
    }

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS datasets (
        "DataSetId" TEXT,
        "DataSetName" TEXT,
        "MeasureCode" TEXT,
        "ReportedMeasureCode" TEXT,
        "ReportingStartDate" TEXT
    )
    """
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute(create_table_sql)

        for row in data_frame.collect():
            cursor.execute(
                """
                INSERT INTO prova ("DataSetId", "DataSetName", "MeasureCode", "ReportedMeasureCode", "ReportingStartDate")
                VALUES (%s, %s, %s, %s, %s)
                """, 
                (row['DataSetId'], row['DataSetName'], row['MeasureCode'], row['ReportedMeasureCode'], row['ReportingStartDate'])
            )
        
        conn.commit()
        logging.info("Data inserted into PostgreSQL.")
    except Exception as e:
        logging.error(f"Failed to insert data into PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    try:
        csv_files = download_datasets_csv()
        if csv_files:
            send_to_rabbitmq(csv_files)
            consume_from_rabbitmq()
        else:
            logging.info("No datasets found.")
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
