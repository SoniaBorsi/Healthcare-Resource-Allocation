import pandas as pd
import requests
import dask.dataframe as dd

# def get_unique_reported_measure_names():
#     url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#         'User-Agent': 'MyApp/1.0',
#         'accept': 'text/csv'
#     }
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         with open('datasets.csv', 'w') as f:
#             f.write(response.text)

#         datasets = dd.read_csv("datasets.csv")
#         datasets['ReportedMeasureName'] = datasets['ReportedMeasureName'].str.strip()
#         unique_reported_measure_names = datasets['ReportedMeasureName'].drop_duplicates().compute().tolist()

#         return unique_reported_measure_names
#     else:
#         print("Failed to fetch data. Status code:", response.status_code)

# unique_reported_measure_names = get_unique_reported_measure_names()
# dataframe = pd.DataFrame(unique_reported_measure_names)
# dataf = dataframe.to_csv("reported_measures_names.csv", header=True)
# print(dataf)



# def get_unique_reported_measure_code():
#     url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
#     headers = {
#         'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
#         'User-Agent': 'MyApp/1.0',
#         'accept': 'text/csv'
#     }
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         with open('datasets.csv', 'w') as f:
#             f.write(response.text)

#         datasets = dd.read_csv("datasets.csv")
#         datasets['ReportedMeasureCode'] = datasets['ReportedMeasureCode'].str.strip()
#         unique_measure_names = datasets['ReportedMeasureCode'].drop_duplicates().compute().tolist()

#         return unique_measure_names
#     else:
#         print("Failed to fetch data. Status code:", response.status_code)

# unique_measure_code = get_unique_reported_measure_code()
# dataframe = pd.DataFrame(unique_measure_code)
# dataf = dataframe.to_csv("measures_names.csv", header=True)
# print(dataf)


import requests
import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine

def get_unique_reported_measure_codes():
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
        datasets['MeasureName'] = datasets['MeasureName'].str.strip()
        datasets['ReportedMeasureName'] = datasets['ReportedMeasureName'].str.strip()

        unique_reported_measure_codes = datasets[['ReportedMeasureName', 'MeasureName']].drop_duplicates().compute()

        return unique_reported_measure_codes
    else:
        print("Failed to fetch data. Status code:", response.status_code)

unique_reported_measure_codes = get_unique_reported_measure_codes()

# Create SQLAlchemy Engine
conn_string = 'postgresql://myuser:mypassword@localhost:5432/mydatabase'
engine = create_engine(conn_string)

# Convert DataFrame to SQL directly
unique_reported_measure_codes.to_sql('measure', engine, if_exists='replace', index=False)
