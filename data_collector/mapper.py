import requests
import pandas as pd
import psycopg2

def map_hospitals(engine):

    print('Fetching Hospitals data...')

    url = "https://myhospitalsapi.aihw.gov.au/api/v1/reporting-units-downloads/mappings"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'application/json'
    }

    response = requests.get(url, headers=headers)

    filename = 'hospital_mapping.xlsx'

    with open(filename, 'wb') as file:
        file.write(response.content)

    df = pd.read_excel(filename,engine='openpyxl', skiprows=3)

    print('Fetching successful, writing to db')

    # Write DataFrame to PostgreSQL

    table_name = 'hospital_map'
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    print(f"Hospital mapping successfully written to table '{table_name}' in the database.")




