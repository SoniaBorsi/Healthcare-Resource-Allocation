import pandas as pd
import requests
import dask.dataframe as dd

def get_unique_reported_measure_names():
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
        unique_reported_measure_names = datasets['ReportedMeasureName'].drop_duplicates().compute().tolist()

        return unique_reported_measure_names
    else:
        print("Failed to fetch data. Status code:", response.status_code)

unique_reported_measure_names = get_unique_reported_measure_names()
dataframe = pd.DataFrame(unique_reported_measure_names)
dataf = dataframe.to_csv("reported_measures_names.csv", header=True)
print(dataf)



def get_unique_measure_names():
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
        datasets['ReportedMeasureName'] = datasets['MeasureName'].str.strip()
        unique_measure_names = datasets['MeasureName'].drop_duplicates().compute().tolist()

        return unique_measure_names
    else:
        print("Failed to fetch data. Status code:", response.status_code)

unique_measure_names = get_unique_measure_names()
dataframe = pd.DataFrame(unique_measure_names)
dataf = dataframe.to_csv("measures_names.csv", header=True)
print(dataf)


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
        datasets['MeasureCode'] = datasets['MeasureCode'].str.strip()
        datasets['ReportedMeasureName'] = datasets['ReportedMeasureName'].str.strip()
        datasets['MeasureName'] = datasets['MeasureName'].str.strip()

        unique_reported_measure_codes = datasets[['MeasureCode', 'ReportedMeasureName', 'MeasureName']].drop_duplicates().compute()

        return unique_reported_measure_codes
    else:
        print("Failed to fetch data. Status code:", response.status_code)

unique_reported_measure_codes = get_unique_reported_measure_codes()
unique_reported_measure_codes.to_csv("all.csv", header=True, index=False)
print(unique_reported_measure_codes)
