import requests
import pandas as pd
import dask.dataframe as dd
import tempfile
from io import StringIO

def get_hospitals_series_id():
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
        'User-Agent': 'MyApp/1.0',
        'accept' : 'text/csv'
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

hospitals_series_id = get_hospitals_series_id()
if hospitals_series_id is not None:
    print(hospitals_series_id)


def download_datasets(num_datasets_to_download, dataset_ids = hospitals_series_id):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
    'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
    'User-Agent': 'MyApp/1.0',
    'accept': 'text/csv'
    }
    for dataset_id in dataset_ids[:num_datasets_to_download]:
        url = f"{base_url}{dataset_id}/data-items"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:

            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                temp_file.write(response.text)
                temp_file_path = temp_file.name

            ddf = dd.read_csv(temp_file_path, dtype={'Caveats': 'object', 'Suppressions': 'object'})

            print(f"Dataset ID: {dataset_id}")
            print(ddf.head())

        else:
            print(f"Error fetching dataset with ID {dataset_id}:")
            print("Status Code:", response.status_code)
            print("Response Headers:", response.headers)
            print("Response Text:", response.text)


num_datasets_to_download = 5
download_datasets(num_datasets_to_download) 