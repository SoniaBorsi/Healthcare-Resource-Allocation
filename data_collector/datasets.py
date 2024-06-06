import requests
import logging
import tempfile
import pika
import psycopg2
from pyspark.sql import SparkSession
from data_collector.values import consume_from_rabbitmq, send_to_rabbitmq, callback

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
            file_path = 'datasets.csv'
            with open(file_path, 'wb') as f:  # Open file in binary mode
                f.write(response.content)
            logging.info("CSV file downloaded successfully and saved as 'datasets.csv'.")
            return [file_path]  # Return as a list to be compatible with send_to_rabbitmq
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching datasets list: {e}")
        return None

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
                INSERT INTO datasets ("DataSetId", "DataSetName", "MeasureCode", "ReportedMeasureCode", "ReportingStartDate")
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

def send_to_rabbitmq(csv_files):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='selected_dataset_queue')

        for csv_file in csv_files:
            with open(csv_file, 'r') as file:
                csv_data = file.read()
                channel.basic_publish(exchange='', routing_key='selected_dataset_queue', body=csv_data)

        connection.close()
        logging.info("CSV files sent to RabbitMQ.")
    except Exception as e:
        logging.error(f"Failed to send CSV files to RabbitMQ: {e}")

def callback(ch, method, properties, body):
    try:
        csv_data = body.decode('utf-8')

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        spark = SparkSession.builder.appName("Healthcare-Resource-Allocation").getOrCreate()
        sdf = spark.read.csv(temp_file_path, header=True, inferSchema=True)

        insert_into_postgresql(sdf)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Message processed and acknowledged.")
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

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
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")

if __name__ == "__main__":
    try:
        csv_file_paths = download_datasets_csv()
        if csv_file_paths:
            send_to_rabbitmq(csv_file_paths)
            consume_from_rabbitmq()
        else:
            logging.info("No datasets found.")
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
