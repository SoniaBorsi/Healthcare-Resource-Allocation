import psycopg2
import requests
import tempfile
import pandas as pd
import dask.dataframe as dd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pika
import csv 
import json


#with dask 

#FUCTION TO EXTRACT ALL THE IDS
import requests
import dask.dataframe as dd

# Function to get hospital series IDs
def get_hospitals_series_id():
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open('datasets.csv', 'w') as f:
            f.write(response.text)
        datasets = dd.read_csv("datasets.csv")
        hospitals_series_id = datasets['DataSetId'].compute()
        return hospitals_series_id
    else:
        print("Failed to fetch data. Status code:", response.status_code)
        return None

# Function to get specific hospital dataset IDs
def get_hospitals_selected_id(ReportedMeasureCode, ReportedMeasureName, ReportingStartDate=None):
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open('selected_datasets.csv', 'w') as f:
            f.write(response.text)
        
        datasets = dd.read_csv("selected_datasets.csv")
        if ReportingStartDate is not None:
            filtered_datasets = datasets[
                (datasets['ReportedMeasureCode'] == ReportedMeasureCode) &
                (datasets['ReportedMeasureName'] == ReportedMeasureName) &
                (datasets['ReportingStartDate'] == ReportingStartDate)
            ]
        else:
            filtered_datasets = datasets[
                (datasets['ReportedMeasureCode'] == ReportedMeasureCode) &
                (datasets['ReportedMeasureName'] == ReportedMeasureName)
            ]
        
        result = filtered_datasets.compute()
        dataset_ids = result['DataSetId']
        
        return dataset_ids
    else:
        print("Failed to fetch data. Status code:", response.status_code)
        return None

# Function to download dataset by ID
def download_dataset_by_id(dataset_id):
    url = f"https://myhospitalsapi.aihw.gov.au/api/v1/datasets/{dataset_id}"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        file_name = f"dataset_{dataset_id}.csv"
        with open(file_name, 'w') as f:
            f.write(response.text)
        print(f"Dataset {dataset_id} downloaded successfully as {file_name}.")
    else:
        print(f"Failed to download dataset {dataset_id}. Status code:", response.status_code)


hospitals_selected_id = get_hospitals_selected_id("MYH-RM0001", "Non-Urgent", "2012-07-01")
if hospitals_selected_id is not None:
    print(hospitals_selected_id)



# #STORE IN THE DB
# def store_in_db(dataset_ids):
#     base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#         'User-Agent': 'MyApp/1.0',
#         'accept': 'text/csv'
#     }
    
#     for dataset_id in dataset_ids:
#         url = f"{base_url}{dataset_id}/data-items"
#         response = requests.get(url, headers=headers)

#         if response.status_code == 200:
#             with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#                 temp_file.write(response.text)
#                 temp_file_path = temp_file.name

#             ddf = dd.read_csv(temp_file_path, dtype={'Caveats': 'object', 'Suppressions': 'object'})

#             conn_string = 'postgresql://myuser:mypassword@localhost:5432/mydatabase'
#             engine = create_engine(conn_string)

#             table_name = f"dataset_{dataset_id}" 

#             with psycopg2.connect(
#                 database="mydatabase", 
#                 user='myuser', 
#                 password='mypassword', 
#                 host='localhost', 
#                 port='5432'
#             ) as conn1:
#                 conn1.autocommit = True
#                 with conn1.cursor() as cursor:
#                     cursor.execute(f'DROP TABLE IF EXISTS {table_name}')

#                 ddf.to_sql(table_name, conn_string, if_exists='replace', index=False)
        
#         else:
#             print(f"Error fetching dataset with ID {dataset_id}:")
#             print("Status Code:", response.status_code)
#             print("Response Headers:", response.headers)
#             print("Response Text:", response.text)



        
# filtered_datasets = get_hospitals_selected_id("MYH-RM0021", "Non-Urgent", "2012-07-01")
# store_in_db(hospitals_selected_id)


#VERSIONE 2 RABBIT MQ

#PRODUCER
# def get_hospitals_selected_id(ReportedMeasureCode, ReportedMeasureName, ReportingStartDate=None):
#     url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#         'User-Agent': 'MyApp/1.0',
#         'accept': 'text/csv'
#     }
    
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         with open('selected_datasets.csv', 'w') as f:
#             f.write(response.text)
        
#         datasets = dd.read_csv("selected_datasets.csv")
#         if ReportingStartDate is not None:
#             filtered_datasets = datasets[
#                 (datasets['ReportedMeasureCode'] == ReportedMeasureCode) &
#                 (datasets['ReportedMeasureName'] == ReportedMeasureName) &
#                 (datasets['ReportingStartDate'] == ReportingStartDate)
#             ]
#         else:
#             filtered_datasets = datasets[
#                 (datasets['ReportedMeasureCode'] == ReportedMeasureCode) &
#                 (datasets['ReportedMeasureName'] == ReportedMeasureName)]
        
#         result = filtered_datasets.compute()
#         dataset_ids = result['DataSetId'].tolist()
        
#         return dataset_ids
#     else:
#         print("Failed to fetch data. Status code:", response.status_code)
#         return None



# def send_to_queue(dataset_ids, queue_name):
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
    
#     channel.queue_declare(queue=queue_name)
    
#     for dataset_id in dataset_ids:
#         message = json.dumps({'dataset_id': dataset_id})
#         channel.basic_publish(exchange='',
#                               routing_key=queue_name,
#                               body=message)
#         print(f"Sent dataset ID {dataset_id} to queue '{queue_name}'")
    
#     connection.close()

# hospitals_selected_id = get_hospitals_selected_id("MYH-RM0021", "Non-Urgent", "2012-07-01")
# if hospitals_selected_id is not None:
#     send_to_queue(hospitals_selected_id, 'ok_queue')



# # CONSUMER
# def store_in_db(dataset_id):
#     base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#         'User-Agent': 'MyApp/1.0',
#         'accept': 'text/csv'
#     }
    
#     url = f"{base_url}{dataset_id}/data-items"
#     response = requests.get(url, headers=headers)

#     if response.status_code == 200:
#         with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#             temp_file.write(response.text)
#             temp_file_path = temp_file.name

#         ddf = dd.read_csv(temp_file_path, dtype={'Caveats': 'object', 'Suppressions': 'object'})

#         conn_string = 'postgresql://myuser:mypassword@localhost:5432/mydatabase'
#         engine = create_engine(conn_string)

#         table_name = f"dataset_{dataset_id}" 

#         with psycopg2.connect(
#             database="mydatabase", 
#             user='myuser', 
#             password='mypassword', 
#             host='localhost', 
#             port='5432'
#         ) as conn1:
#             conn1.autocommit = True
#             with conn1.cursor() as cursor:
#                 cursor.execute(f'DROP TABLE IF EXISTS {table_name}')

#             ddf.to_sql(table_name, conn_string, if_exists='replace', index=False)
    
#     else:
#         print(f"Error fetching dataset with ID {dataset_id}:")
#         print("Status Code:", response.status_code)
#         print("Response Headers:", response.headers)
#         print("Response Text:", response.text)


# def callback(ch, method, properties, body):
#     data = json.loads(body)
#     dataset_id = data['dataset_id']
#     print(f"Received dataset ID {dataset_id} from queue")
#     store_in_db(dataset_id)
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def consume_from_queue(queue_name):
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     channel.queue_declare(queue=queue_name)
#     channel.basic_consume(queue=queue_name, on_message_callback=callback)

#     print(f'Waiting for messages in queue: {queue_name}. To exit press CTRL+C')
#     channel.start_consuming()  #


# consume_from_queue('dataset_queue') 
