import requests
import dask.dataframe as dd
import pandas as pd

def map_hospitals():

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

    return df

data = map_hospitals()

import psycopg2 
import pandas as pd 
from sqlalchemy import create_engine 

conn_string = 'postgresql://myuser:mypassword@localhost:5432/mydatabase'
engine = create_engine(conn_string)

conn1 = psycopg2.connect( 
	database="mydatabase", 
    user='myuser', 
    password='mypassword', 
    host='localhost', 
    port= '5432'
    ) 

conn1.autocommit = True
cursor = conn1.cursor() 

cursor.execute('drop table if exists prova') 
data.to_sql('prova', engine, if_exists='replace', index=False)

conn1.commit() 
conn1.close() 

