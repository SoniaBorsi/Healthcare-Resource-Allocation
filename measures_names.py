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
        return None

unique_reported_measure_names = get_unique_reported_measure_names()

if unique_reported_measure_names is not None:
    df_unique_names = pd.DataFrame({'ReportedMeasureName': unique_reported_measure_names})
    df_unique_names.to_csv('unique_reported_measure_names.csv', index=False)

    print("DataFrame with Unique Reported Measure Names:")
    print(df_unique_names)
else:
    print("Failed to fetch unique reported measure names.")