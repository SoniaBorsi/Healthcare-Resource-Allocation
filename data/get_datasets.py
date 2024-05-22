import psycopg2
import requests
import tempfile
import pandas as pd
import dask.dataframe as dd
from pyspark.sql import SparkSession


#with dask 

# FUCTION TO EXTRACT ALL THE IDS
# def get_hospitals_series_id():
#     url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
#         'User-Agent': 'MyApp/1.0',
#         'accept' : 'text/csv'
#     }
    
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         with open('datasets.csv', 'w') as f:
#             f.write(response.text)
#         datasets = dd.read_csv("datasets.csv")
#         hospitals_series_id = datasets['DataSetId'].compute()
#         return hospitals_series_id
#     else:
#         print("Failed to fetch data. Status code:", response.status_code)
#         return None


# hospitals_series_id = get_hospitals_series_id()
# if hospitals_series_id is not None:
#     print(hospitals_series_id)


# def get_hospitals_selected_id(ReportedMeasureCode, ReportedMeasureName, ReportingStartDate):
#     url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
#         'User-Agent': 'MyApp/1.0',
#         'accept' : 'text/csv'
#     }
    
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         with open('selected_datasets.csv', 'w') as f:
#             f.write(response.text)
        
#         # Load the CSV file using Dask
#         datasets = dd.read_csv("selected_datasets.csv")
        
#         # Apply filters
#         filtered_datasets = datasets[
#             (datasets['ReportedMeasureCode'] == ReportedMeasureCode) &
#             (datasets['ReportedMeasureName'] == ReportedMeasureName) &
#             (datasets['ReportingStartDate'] == ReportingStartDate)
#         ]
        
#         result = filtered_datasets.compute()
#         dataset_ids = result['DataSetId']
        
#         return dataset_ids
#     else:
#         print("Failed to fetch data. Status code:", response.status_code)
#         return None

# hospitals_selected_id = get_hospitals_selected_id("MYH-RM0001", "Breast cancer", "2011-07-01")
# if hospitals_selected_id is not None:
#     print(hospitals_selected_id)


# def download_datasets(num_datasets_to_download, dataset_id):
#     base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#     'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#     'User-Agent': 'MyApp/1.0',
#     'accept': 'text/csv'
#     }
#     for dataset_id in dataset_id[:num_datasets_to_download]:
#         url = f"{base_url}{dataset_id}/data-items"
#         response = requests.get(url, headers=headers)

#         if response.status_code == 200:
#             with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#                 temp_file.write(response.text)
#                 temp_file_path = temp_file.name

#             ddf = dd.read_csv(temp_file_path, dtype={'Caveats': 'object', 'Suppressions': 'object'})

#             print(f"Dataset ID: {dataset_id}")
#             print(ddf.head())

#             ddf.to_csv(f'{dataset_id}.csv', single_file=True, index=False)
#         else:
#             print(f"Error fetching dataset with ID {dataset_id}:")
#             print("Status Code:", response.status_code)
#             print("Response Headers:", response.headers)
#             print("Response Text:", response.text)

# download_datasets(1, hospitals_selected_id)




#with spark

def get_hospitals_selected_id(ReportedMeasureCode, ReportedMeasureName, ReportingStartDate):
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
        'User-Agent': 'MyApp/1.0',
        'accept' : 'text/csv'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            temp_file.write(response.text)
            temp_file_path = temp_file.name
        
        spark = SparkSession.builder.appName("MyHospitals").getOrCreate()
        
        datasets = spark.read.csv(temp_file_path, header=True)
        
        filtered_datasets = datasets.filter(
            (datasets['ReportedMeasureCode'] == ReportedMeasureCode) &
            (datasets['ReportedMeasureName'] == ReportedMeasureName) &
            (datasets['ReportingStartDate'] == ReportingStartDate)
        )
        
        dataset_ids = filtered_datasets.select('DatasetId').rdd.flatMap(lambda x: x).collect()
        
        spark.stop()  
        
        return dataset_ids
    else:
        print("Failed to fetch data. Status code:", response.status_code)
        return None

hospitals_selected_id = get_hospitals_selected_id("MYH-RM0001", "Breast cancer", "2011-07-01")
if hospitals_selected_id is not None:
    print(hospitals_selected_id)

def download_datasets(num_datasets_to_download, dataset_id):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    for dataset_id in dataset_id[:num_datasets_to_download]:
        url = f"{base_url}{dataset_id}/data-items"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            with open(f"{dataset_id}.csv", "w") as csv_file:
                csv_file.write(response.text)
        else:
            print(f"Error fetching dataset with ID {dataset_id}:")
            print("Status Code:", response.status_code)
            print("Response Headers:", response.headers)
            print("Response Text:", response.text)

download_datasets(1, hospitals_selected_id)
