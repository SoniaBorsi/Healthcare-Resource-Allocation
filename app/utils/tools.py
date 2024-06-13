import logging
import pandas as pd
import pika
import time
from tqdm import tqdm
import requests


def map_hospitals(spark_session):
    print('Fetching Hospitals data...')
    
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/reporting-units-downloads/mappings"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'application/json'
    }

    response = requests.get(url, headers=headers)
    filename = 'hospital_mapping.xlsx'

    with open(filename, 'wb') as file:
        file.write(response.content)

    df = pd.read_excel(filename, engine='openpyxl', skiprows=3)

    sdf = spark_session.createDataFrame(df)   

    insert_into_postgresql(sdf, "hospitals")
    print("Hospital mapping inserted successfully into the PostgreSQL database")


def get_ids(file_path):
    datasets = pd.read_csv(file_path)
    hospitals_series_id = datasets['DataSetId'].tolist()
    return hospitals_series_id

def insert_into_postgresql(data_frame, table_name):
    url = "jdbc:postgresql://postgres:5432/mydatabase"
    properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    try:
        data_frame.write.jdbc(url=url, table=table_name, mode="append", properties=properties)
    except Exception as e:
        logging.error(f"Failed to insert data into PostgreSQL: {e}")

def send_to_rabbitmq(concatenated_csv):
    connection_attempts = 0
    max_attempts = 5
    while connection_attempts < max_attempts:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()
            channel.queue_declare(queue='values_queue')
            channel.basic_publish(exchange='', routing_key='values_queue', body=concatenated_csv)
        
            logging.info("Data sent to RabbitMQ.")
            connection.close()
            return
        except Exception as e:
            logging.error(f"Failed to send CSV files to RabbitMQ: {e}")
            connection_attempts += 1
            time.sleep(5)
    logging.error("Exceeded maximum attempts to connect to RabbitMQ.")
    
def consume_from_rabbitmq(spark_session, queue_name, callback_function):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        queue_declare_result = channel.queue_declare(queue=queue_name, passive=True)
        total_messages = queue_declare_result.method.message_count

        with tqdm(total=total_messages, desc=f'Consuming from queue "{queue_name}"') as pbar:

            def on_message_callback(ch, method, properties, body):
                callback_function(spark_session, ch, method, properties, body)
                pbar.update(1)
                if pbar.n >= pbar.total:  
                    logging.info("Progress bar is full. Stopping consumption.")
                    channel.stop_consuming()

            channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)
            logging.info(f'[*] Waiting for messages on queue "{queue_name}". To exit press CTRL+C')
            channel.start_consuming()

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected. Stopping consumption.")
        channel.stop_consuming()
    except Exception as e:
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")
    finally:
        connection.close()

def download_datasetlist_csv(spark_session):
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            file_path = 'datasets.csv'
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logging.info("List of available data retrieved")

            df = pd.read_csv(file_path)
            
            sdf = spark_session.createDataFrame(df)
        
            reportedmeasurements = sdf.select('ReportedMeasureCode', 'ReportedMeasureName')
            measurements = sdf.select('MeasureCode', 'MeasureName')
            values = sdf.select('ReportingStartDate', 'ReportedMeasureCode', 'DataSetId', 'MeasureCode', 'DatasetName')
        
            insert_into_postgresql(reportedmeasurements, "reported_measurements")
            insert_into_postgresql(measurements, "measurements")
            insert_into_postgresql(values, "datasets")

            logging.info("Tables datasets, measurements and reported_measurements inserted in the db")

            return file_path
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching datasets list: {e}")
        return None
