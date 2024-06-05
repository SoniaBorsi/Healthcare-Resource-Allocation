import requests
import pandas as pd
import dask.dataframe as dd
import pika
import json
import psycopg2

import requests
import pandas as pd
import requests
import pandas as pd
import tempfile
import pika
import psycopg2
from io import StringIO
import json

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
            # Write the CSV response to a temporary file
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                temp_file.write(response.text)
                temp_file_path = temp_file.name
            
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(temp_file_path, dtype={'Caveats': 'object', 'Suppressions': 'object'})
            dfs.append(df)
    
    # Concatenate all DataFrames into one
    if dfs:
        result_df = pd.concat(dfs, ignore_index=True)
        return result_df
    else:
        return None

def send_to_rabbitmq(dataset):
    # Establish connection to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare a queue named 'selected_dataset_queue'
    channel.queue_declare(queue='selected_dataset_queue')

    # Convert dataset to JSON string
    json_data = dataset.to_json(orient='records')

    # Publish data to the queue
    channel.basic_publish(exchange='', routing_key='selected_dataset_queue', body=json_data)

    # Close connection
    connection.close()

def insert_into_postgresql(data_frame):
    # Database connection parameters
    conn_params = {
        'dbname': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': 5432
    }
    
    # Establish connection
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    # Iterate through DataFrame and insert each row
    for index, row in data_frame.iterrows():
        cursor.execute(
            """
            INSERT INTO dataset_62 (\"DataSetId\", \"GroupNumber\", \"LowerValue\", \"MeasureCode\", \"PeerGroupCode\",
                                   \"PeerGroupName\", \"PeerGroupTypeCode\", \"PeerGroupTypeName\", \"ProxyReportingUnitCode\",
                                   \"ProxyReportingUnitName\", \"ProxyReportingUnitTypeCode\", \"ProxyReportingUnitTypeName\",
                                   \"ReportedMeasureCode\", \"ReportingUnitCode\", \"ReportingUnitName\",
                                   \"ReportingUnitTypeCode\", \"ReportingUnitTypeName\", \"UpperValue\", \"Value\",
                                   \"Caveats\", \"Suppressions\")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, 
            (row['DataSetId'], row['GroupNumber'], row['LowerValue'], row['MeasureCode'], row['PeerGroupCode'], row['PeerGroupName'],
             row['PeerGroupTypeCode'], row['PeerGroupTypeName'], row['ProxyReportingUnitCode'], row['ProxyReportingUnitName'],
             row['ProxyReportingUnitTypeCode'], row['ProxyReportingUnitTypeName'], row['ReportedMeasureCode'], row['ReportingUnitCode'],
             row['ReportingUnitName'], row['ReportingUnitTypeCode'], row['ReportingUnitTypeName'], row['UpperValue'], row['Value'],
             row['Caveats'], row['Suppressions'])
        )
    
    # Commit the transaction
    conn.commit()
    
    # Close the connection
    cursor.close()
    conn.close()

def callback(ch, method, properties, body):
    # Decode the JSON data
    data_json = body.decode('utf-8')

    # Convert JSON string to DataFrame
    data_frame = pd.read_json(StringIO(data_json), orient='records')

    # Insert into PostgreSQL
    insert_into_postgresql(data_frame)

    # Acknowledge the message was processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_from_rabbitmq():
    # Establish connection to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare a queue named 'selected_dataset_queue'
    channel.queue_declare(queue='selected_dataset_queue')

    # Set up subscription on the queue
    channel.basic_consume(queue='selected_dataset_queue', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    # Example dataset IDs
    dataset_ids = ['1', '2']

    # Step 1: Get datasets
    datasets_df = get_datasets(dataset_ids)
    if datasets_df is not None:
        # Step 2: Send to RabbitMQ
        send_to_rabbitmq(datasets_df)

        # Step 3: Consume from RabbitMQ and insert into PostgreSQL
        consume_from_rabbitmq()
    else:
        print("No datasets found.")
