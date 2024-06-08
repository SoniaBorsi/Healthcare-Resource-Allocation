import requests
import pandas as pd
from sqlalchemy import create_engine

def map_hospitals():
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

    df = pd.read_excel(filename, engine='openpyxl', skiprows=3)
    engine = create_engine('postgresql+psycopg2://myuser:mypassword@localhost/mydatabase')
    df.to_sql('mapper', engine, if_exists='replace', index=False)
    print("Data inserted successfully into the PostgreSQL database")

map_hospitals()




