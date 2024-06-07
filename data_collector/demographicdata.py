import requests
import pandas as pd 
from io import StringIO 

#API_key = 'yavM2fmARyaktqwUh9UiL5b4ijS0jVBZ3ZiSlCyG'

base_url = 'https://api.data.abs.gov.au/dataflow/abs'

headers = {
   # 'x-api-key': API_key,
    'format' : 'csv'
}

response = requests.get(base_url, headers=headers)

print(response.content)



