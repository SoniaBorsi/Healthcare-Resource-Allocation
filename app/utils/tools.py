import logging
import pandas as pd
import pika
import time
import requests
from sqlalchemy import create_engine

def map_hospitals():
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
    
    engine = create_engine('postgresql+psycopg2://user:password@postgres:5432/mydatabase')
    df.to_sql('hospitals', engine, if_exists='replace', index=False)
    print("Hospital mapping inserted successfully into the PostgreSQL database")


def insert_into_postgresql(data_frame, table_name):
    url = "jdbc:postgresql://localhost:5432/mydatabase"
    properties = {
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }

    try:
        data_frame.write.jdbc(url=url, table=table_name, mode="append", properties=properties)
        logging.info("Data inserted into PostgreSQL.")
    except Exception as e:
        logging.error(f"Failed to insert data into PostgreSQL: {e}")

def get_ids():
    datasets = pd.read_csv('datasets.csv')
    hospitals_series_id = datasets['DataSetId'].tolist()
    return hospitals_series_id

def send_to_rabbitmq(csv_files):
    connection_attempts = 0
    max_attempts = 5
    while connection_attempts < max_attempts:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            if len(csv_files) > 1:
                channel.queue_declare(queue='values_queue')
                for csv_file in csv_files:
                    with open(csv_file, 'r') as file:
                        csv_data = file.read()
                        channel.basic_publish(exchange='', routing_key='values_queue', body=csv_data)
            else:
                channel.queue_declare(queue='datasets_measurements_reportedmeasurements_queue')
                for csv_file in csv_files:
                    with open(csv_file, 'r') as file:
                        csv_data = file.read()
                        channel.basic_publish(exchange='', routing_key='datasets_measurements_reportedmeasurements_queue', body=csv_data)

            connection.close()
            logging.info("CSV files sent to RabbitMQ.")
            return
        except Exception as e:
            logging.error(f"Failed to send CSV files to RabbitMQ: {e}")
            connection_attempts += 1
            time.sleep(5)
    logging.error("Exceeded maximum attempts to connect to RabbitMQ.")
