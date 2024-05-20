import requests
import dask.dataframe as dd
import pandas as pd
import tempfile

# Define the URL and headers
url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/10352/data-items"
headers = {
    'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
    'User-Agent': 'MyApp/1.0',
    'accept': 'text/csv'
}

# Make the request
response = requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Save the response content to a temporary file
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(response.text)
        temp_file_path = temp_file.name

    # Read the CSV data into a Dask DataFrame
    ddf = dd.read_csv(temp_file_path)

    # Extract the 'ReportingUnitName' column and compute the result
    hospitals_series = ddf['ReportingUnitName'].compute()

    # Convert the series to a pandas DataFrame
    hospitals_df = pd.DataFrame(hospitals_series, columns=['ReportingUnitName'])

    # Print the first few rows of the hospitals DataFrame
    print(hospitals_df)

else:
    # Print error details
    print("Error:", response.status_code)
    print("Response Headers:", response.headers)
    print("Response Text:", response.text)

