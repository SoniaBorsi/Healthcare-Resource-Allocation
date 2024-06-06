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

# # Function to download dataset by ID
# def download_dataset_by_id(dataset_id):
#     url = f"https://myhospitalsapi.aihw.gov.au/api/v1/datasets/{dataset_id}"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
#         'User-Agent': 'MyApp/1.0',
#         'accept': 'text/csv'
#     }
    
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         file_name = f"dataset_{dataset_id}.csv"
#         with open(file_name, 'w') as f:
#             f.write(response.text)
#         print(f"Dataset {dataset_id} downloaded successfully as {file_name}.")
#     else:
#         print(f"Failed to download dataset {dataset_id}. Status code:", response.status_code)


# hospitals_selected_id = get_hospitals_selected_id("MYH-RM0001", "Non-Urgent", "2012-07-01")
# if hospitals_selected_id is not None:
#     print(hospitals_selected_id)



