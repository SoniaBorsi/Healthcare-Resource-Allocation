import logging
import tempfile
from pyspark.sql import SparkSession
import requests
import pika
import pandas as pd
import time
from math import ceil

logging.basicConfig(level=logging.INFO)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Healthcare-Resource-Allocation") \
        .config("spark.driver.extraClassPath", "/Users/soniaborsi/Desktop/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark

def get_ids():
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(response.text)
            temp_file_path = temp_file.name
    
        datasets = pd.read_csv(temp_file_path)
        hospitals_series_id = datasets['DataSetId'].tolist()
        return hospitals_series_id
    else:
        print("Failed to fetch data. Status code:", response.status_code)
        return None

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
        channel.queue_declare(queue='values_queue')

        for csv_file in csv_files:
            with open(csv_file, 'r') as file:
                csv_data = file.read()
                channel.basic_publish(exchange='', routing_key='values_queue', body=csv_data)

        connection.close()
        logging.info("CSV files sent to RabbitMQ.")
    except Exception as e:
        logging.error(f"Failed to send CSV files to RabbitMQ: {e}")

def insert_into_postgresql(data_frame):
    url = "jdbc:postgresql://localhost:5432/mydatabase"
    properties = {
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }

    try:
        data_frame.write.jdbc(url=url, table="values", mode="append", properties=properties)
        logging.info("Data inserted into PostgreSQL.")
    except Exception as e:
        logging.error(f"Failed to insert data into PostgreSQL: {e}")


def callback(ch, method, properties, body, spark):
    try:
        csv_data = body.decode('utf-8')
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        spark = create_spark_session()
        sdf = spark.read.csv(temp_file_path, header=True, inferSchema=True)
        values = sdf.select('DataSetId', 'ReportingUnitCode', 'Value', 'Caveats')

        insert_into_postgresql(values)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Message processed and acknowledged.")
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

def consume_from_rabbitmq(spark):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='values_queue')
        on_message_callback = lambda ch, method, properties, body: callback(ch, method, properties, body, spark)
        channel.basic_consume(queue='values_queue', on_message_callback=on_message_callback)
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
    except Exception as e:
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")

        
if __name__ == "__main__":
    spark = create_spark_session()
    try:
        all_dataset_ids = get_ids()  
        if all_dataset_ids:
            batch_size = 100
            num_batches = ceil(len(all_dataset_ids) / batch_size)
            for i in range(num_batches):
                batch_ids = all_dataset_ids[i * batch_size:(i + 1) * batch_size]
                csv_files = get_datasets(batch_ids)
                if csv_files:
                    send_to_rabbitmq(csv_files)
                    consume_from_rabbitmq(spark)
                else:
                    logging.info(f"No datasets found in batch {i + 1}.")
        else:
            logging.info("Failed to fetch dataset IDs.")
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')