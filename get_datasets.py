import requests
import dask.dataframe as dd
import tempfile
import psycopg2
from psycopg2.extras import execute_values

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
        hospitals = datasets[(datasets['ReportedMeasureName'] == 'All patients') | 
                             (datasets['ReportedMeasureName'] == 'Total') |
                             (datasets['ReportedMeasureName'] == 'All audited moments')]
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

def download_datasets(num_datasets_to_download, dataset_id_name):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    for dataset_id, dataset_name in list(dataset_id_name.items())[:num_datasets_to_download]:
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

            # Insert data into PostgreSQL
            insert_data_to_postgres(ddf, dataset_name)
        else:
            print(f"Error fetching dataset with ID {dataset_id}:")
            print("Status Code:", response.status_code)
            print("Response Headers:", response.headers)
            print("Response Text:", response.text)

def insert_data_to_postgres(ddf, table_name):
    try:
        conn = psycopg2.connect(
            dbname="hospitals_db",
            user="sonia_borsi",
            password="Jacky1101!",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {" TEXT, ".join(ddf.columns)} TEXT
        );
        """
        cur.execute(create_table_query)

        # Prepare data for insertion
        insert_query = f"INSERT INTO {table_name} ({', '.join(ddf.columns)}) VALUES %s"
        data_to_insert = [tuple(x) for x in ddf.values]

        execute_values(cur, insert_query, data_to_insert)

        conn.commit()
        cur.close()
        conn.close()
        print(f"Data inserted successfully into table {table_name}")
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")

if hospitals_series_id_name is not None:
    download_datasets(1, hospitals_series_id_name)



#OLD VERSION WITHOUT FILTERS IN THE GET SERIES ID FUNCTION 
    
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