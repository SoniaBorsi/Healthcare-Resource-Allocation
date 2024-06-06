import requests
import pandas as pd
import logging
import tempfile
import pika
import psycopg2
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

def get_measures():
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  # Replace with your actual access token
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open('datasets.csv', 'w') as f:
            f.write(response.text)

        datasets = pd.read_csv("datasets.csv")
        datasets['MeasureName'] = datasets['MeasureName'].str.strip()
        datasets['MeasureCode'] = datasets['MeasureCode'].str.strip()

        unique_reported_measure_codes = datasets[['MeasureCode', 'MeasureName']].drop_duplicates()

        return unique_reported_measure_codes
    else:
        logging.error("Failed to fetch data. Status code: %d", response.status_code)
        return None

def insert_into_postgresql(unique_measures):
    conn_params = {
        'dbname': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': 5432
    }

    create_measurements_table_sql = """
    CREATE TABLE IF NOT EXISTS measurements (
        "MeasureCode" TEXT PRIMARY KEY,
        "MeasureName" TEXT
    )
    """

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute(create_measurements_table_sql)

        # Insert into datasets table
        for row in unique_measures.itertuples(index=False):
            cursor.execute(
                """
                INSERT INTO measurements ("MeasureCode", "MeasureName")
                VALUES (%s, %s)
                ON CONFLICT ("MeasureCode") DO NOTHING
                """, 
                (row.MeasureCode, row.MeasureName)
            )
        
        conn.commit()
        logging.info("Data inserted into PostgreSQL.")
    except Exception as e:
        logging.error("Failed to insert data into PostgreSQL: %s", e)
    finally:
        cursor.close()
        conn.close()

def send_to_rabbitmq(unique_measures):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='selected_dataset_queue')

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            unique_measures.to_csv(temp_file.name, index=False)
            temp_file_path = temp_file.name
            logging.info("Temporary CSV file created at: %s", temp_file_path)
        
        with open(temp_file_path, 'r') as file:
            csv_data = file.read()
            channel.basic_publish(exchange='', routing_key='selected_dataset_queue', body=csv_data)

        connection.close()
        logging.info("CSV file sent to RabbitMQ.")
    except Exception as e:
        logging.error("Failed to send CSV file to RabbitMQ: %s", e)

def callback(ch, method, properties, body):
    try:
        csv_data = body.decode('utf-8')

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        spark = SparkSession.builder.appName("Healthcare-Resource-Allocation").getOrCreate()
        sdf = spark.read.csv(temp_file_path, header=True, inferSchema=True)

        sdf = sdf.toPandas()
        sdf['MeasureName'] = sdf['MeasureName'].str.strip()
        sdf['MeasureCode'] = sdf['MeasureCode'].str.strip()
        unique_measures = sdf[['MeasureCode', 'MeasureName']].drop_duplicates()

        insert_into_postgresql(unique_measures)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Message processed and acknowledged.")
    except Exception as e:
        logging.error("Failed to process message: %s", e)

def consume_from_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='selected_dataset_queue')
        channel.basic_consume(queue='selected_dataset_queue', on_message_callback=callback)
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
    except Exception as e:
        logging.error("Failed to consume messages from RabbitMQ: %s", e)

if __name__ == "__main__":
    try:
        unique_measures = get_measures()
        if unique_measures is not None and not unique_measures.empty:
            send_to_rabbitmq(unique_measures)
            consume_from_rabbitmq()
        else:
            logging.info("No datasets found.")
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
