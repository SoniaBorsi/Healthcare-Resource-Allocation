import requests
import dask.dataframe as dd
import tempfile

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
        datasets['ReportedMeasureName'] = datasets['ReportedMeasureName'].str.strip()
        hospitals = datasets[datasets['ReportedMeasureName'] == 'All patients']
        hospitals_data = hospitals[['DataSetId', 'DataSetName']].compute()
        hospitals_series_id_name = dict(zip(hospitals_data['DataSetId'], hospitals_data['DataSetName']))

        return hospitals_series_id_name
    else:
        print("Failed to fetch data. Status code:", response.status_code)
        return None

hospitals_series_id_name = get_hospitals_series_id()
if hospitals_series_id_name is not None:
    print("Filtered DataSet IDs and Names:")
    print(hospitals_series_id_name)
else:
    print("No datasets matched the filter criteria.")



def download_datasets(num_datasets_to_download, dataset_id_name_dict):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    for dataset_id, dataset_name in list(dataset_id_name_dict.items())[:num_datasets_to_download]:
        url = f"{base_url}{dataset_id}/data-items"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                temp_file.write(response.text)
                temp_file_path = temp_file.name

            ddf = dd.read_csv(temp_file_path, dtype={'Caveats': 'object', 'Suppressions': 'object'})

            print(f"Dataset ID: {dataset_id}, Dataset Name: {dataset_name}")
            print(ddf.head())
            sanitized_name = "".join([c if c.isalnum() else "_" for c in dataset_name])  # Sanitize the dataset name
            ddf.to_csv(f'{sanitized_name}.csv', single_file=True, index=False)
        else:
            print(f"Error fetching dataset with ID {dataset_id}:")
            print("Status Code:", response.status_code)
            print("Response Headers:", response.headers)
            print("Response Text:", response.text)

if hospitals_series_id_name is not None:
    download_datasets(1, hospitals_series_id_name)  # Adjust the number as needed


#OLD VERSION 
    
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
        
#         hospitals = datasets[datasets['ReportedMeasureName'] == 'All patients']
#         hospitals_series_id = hospitals['DataSetId'].compute()
#         return hospitals_series_id
#     else:
#         print("Failed to fetch data. Status code:", response.status_code)
#         return None


# hospitals_series_id = get_hospitals_series_id()
# if hospitals_series_id is not None:
#     print(hospitals_series_id)



# def download_datasets(num_datasets_to_download, dataset_ids):
#     base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#     'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#     'User-Agent': 'MyApp/1.0',
#     'accept': 'text/csv'
#     }
#     for dataset_id in dataset_ids[:num_datasets_to_download]:
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


# num_datasets_to_download = 1
# download_datasets(num_datasets_to_download, hospitals_series_id)