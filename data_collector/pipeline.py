import requests
import tempfile
import logging
import pika
import psycopg2
from io import StringIO
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HealthcareDataProcessing") \
    .getOrCreate()

logging.basicConfig(level=logging.INFO)

# def get_datasets(dataset_ids):
#     base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#         'User-Agent': 'MyApp/1.0',
#         'accept': 'text/csv'
#     }

#     csv_files = []
#     for dataset_id in dataset_ids:
#         url = f"{base_url}{dataset_id}/data-items"
#         response = requests.get(url, headers=headers)

#         if response.status_code == 200:
#             with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#                 temp_file.write(response.text)
#                 temp_file_path = temp_file.name
#                 csv_files.append(temp_file_path)
#         else:
#             logging.error(f"Failed to fetch dataset {dataset_id}. Status code: {response.status_code}")
    
#     return csv_files

# def send_to_rabbitmq(csv_files):
#     try:
#         connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#         channel = connection.channel()
#         channel.queue_declare(queue='selected_dataset_queue')

#         for csv_file in csv_files:
#             with open(csv_file, 'r') as file:
#                 csv_data = file.read()
#                 channel.basic_publish(exchange='', routing_key='selected_dataset_queue', body=csv_data)

#         connection.close()
#         logging.info("CSV files sent to RabbitMQ.")
#     except Exception as e:
#         logging.error(f"Failed to send CSV files to RabbitMQ: {e}")


def get_datasets(dataset_ids):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    csv_files = []
    for dataset_id in dataset_ids:
        url = f"{base_url}{dataset_id}/data-items"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    temp_file.write(response.text)
                    temp_file_path = temp_file.name
                    csv_files.append(temp_file_path)
            else:
                logging.error(f"Failed to fetch dataset {dataset_id}. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception occurred while fetching dataset {dataset_id}: {e}")
    
    return csv_files

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

def insert_into_postgresql(data_frame):
    conn_params = {
        'dbname': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': 5432
    }

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS values (
        "DataSetId" TEXT,
        "ReportingUnitCode" TEXT,
        "Value" TEXT,
        "Caveats" TEXT,
        "Suppressions" TEXT
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
                INSERT INTO values ("DataSetId", "ReportingUnitCode", "Value", "Caveats", "Suppressions")
                VALUES (%s, %s, %s, %s, %s)
                """, 
                (row['DataSetId'], row['ReportingUnitCode'], row['Value'], row['Caveats'], row['Suppressions'])
            )
        
        conn.commit()
        logging.info("Data inserted into PostgreSQL.")
    except Exception as e:
        logging.error(f"Failed to insert data into PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

def callback(ch, method, properties, body):
    try:
        csv_data = body.decode('utf-8')

        # Write CSV data to a temporary file
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        # Create a Spark DataFrame from the temporary CSV file
        sdf = spark.read.csv(temp_file_path, header=True, inferSchema=True)

        # Perform Spark transformations (example: filtering, aggregation, etc.)
        # Example: sdf = sdf.filter(sdf['Value'] > 100)

        # Insert processed data into PostgreSQL
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
        dataset_ids = [1,2,3]#list(range(1, 101))  
        csv_files = get_datasets(dataset_ids)
        if csv_files:
            send_to_rabbitmq(csv_files)
            consume_from_rabbitmq()
        else:
            logging.info("No datasets found.")
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')