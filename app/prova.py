import os
import requests
import pandas as pd
import tempfile
import pika
import psycopg2
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)

def get_datasets(dataset_ids):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    dfs = []
    for dataset_id in dataset_ids:
        url = f"{base_url}{dataset_id}/data-items"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                temp_file.write(response.text)
                temp_file_path = temp_file.name
            
            df = pd.read_csv(temp_file_path, dtype={'Caveats': 'object', 'Suppressions': 'object'})
            dfs.append(df)
        else:
            logging.error(f"Failed to fetch dataset {dataset_id}. Status code: {response.status_code}")
    
    if dfs:
        result_df = pd.concat(dfs, ignore_index=True).iloc[:,[0,13,18,19,20]]
        return result_df
    else:
        return None

def send_to_rabbitmq(dataset):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(os.getenv('RABBITMQ_HOST', 'localhost')))
        channel = connection.channel()
        channel.queue_declare(queue='selected_dataset_queue')
        json_data = dataset.to_json(orient='records')
        channel.basic_publish(exchange='', routing_key='selected_dataset_queue', body=json_data)
        connection.close()
        logging.info("Dataset sent to RabbitMQ.")
    except Exception as e:
        logging.error(f"Failed to send dataset to RabbitMQ: {e}")

def insert_into_postgresql(data_frame):
    conn_params = {
        'dbname': os.getenv('POSTGRES_DB', 'mydatabase'),
        'user': os.getenv('POSTGRES_USER', 'myuser'),
        'password': os.getenv('POSTGRES_PASSWORD', 'mypassword'),
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': 5432
    }
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS prova (
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
        
        # Iterate through DataFrame and insert each row
        for index, row in data_frame.iterrows():
            cursor.execute(
                """
                INSERT INTO prova ( "DataSetId", "ReportingUnitCode", "Value",
                                       "Caveats", "Suppressions")
                VALUES (%s, %s, %s, %s, %s)
                """, 
                (row['DataSetId'], row['ReportingUnitCode'], row['Value'],row['Caveats'], row['Suppressions'])
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
        data_json = body.decode('utf-8')
        data_frame = pd.read_json(StringIO(data_json), orient='records')
        insert_into_postgresql(data_frame)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Message processed and acknowledged.")
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

def consume_from_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(os.getenv('RABBITMQ_HOST', 'localhost')))
        channel = connection.channel()
        channel.queue_declare(queue='selected_dataset_queue')
        channel.basic_consume(queue='selected_dataset_queue', on_message_callback=callback)
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except Exception as e:
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")

if __name__ == "__main__":
    dataset_ids = ['5','6','7']
    datasets_df = get_datasets(dataset_ids)
    if datasets_df is not None:
        send_to_rabbitmq(datasets_df)
        consume_from_rabbitmq()
    else:
        logging.info("No datasets found.")
