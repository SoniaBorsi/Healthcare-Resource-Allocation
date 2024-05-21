import requests

# Your API key
API_key = 'yavM2fmARyaktqwUh9UiL5b4ijS0jVBZ3ZiSlCyG'

# Base URL for the API
base_url = 'https://indicator.api.abs.gov.au/v1/data/CPI_M_H/csv'

# Headers containing the API key
headers = {
    'x-api-key': API_key,
    'accept' : 'text/csv'
}

# Make a GET request to the API with headers
response = requests.get(base_url, headers=headers)

print(response.text)
