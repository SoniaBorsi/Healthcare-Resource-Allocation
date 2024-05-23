import requests

API_key = 'yavM2fmARyaktqwUh9UiL5b4ijS0jVBZ3ZiSlCyG'

base_url = 'https://indicator.api.abs.gov.au/v1/data/CPI_M_H/csv'

headers = {
    'x-api-key': API_key,
    'accept' : 'text/csv'
}

response = requests.get(base_url, headers=headers)

print(response.text)


