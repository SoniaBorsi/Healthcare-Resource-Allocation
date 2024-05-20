import requests
import pandas as pd
from io import StringIO

url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
headers = {
    'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
    'User-Agent': 'MyApp/1.0',
    'accept' : 'text/csv'
}

response = requests.get(url, headers=headers)

pd.read_csv(StringIO(response.text)).to_csv('datasets.csv', index=False, header=True)

