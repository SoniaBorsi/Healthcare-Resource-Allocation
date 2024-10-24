import requests
import pandas as pd
import psycopg2
import logging
import pika
import json
import csv
from io import StringIO

# PostgreSQL connection parameters
POSTGRES_PARAMS = {
    "host": "postgres",  # Use the PostgreSQL container/service name if running within Docker
    "port": 5432,  # PostgreSQL default port
    "database": "mydatabase",
    "user": "myuser",
    "password": "mypassword"
}

# RabbitMQ connection parameters
RABBITMQ_PARAMS = {
    "host": "rabbitmq",  # Use the Docker service name for the RabbitMQ container
    "queue": "values"
}

def send_to_rabbitmq(message, queue_name):
    """
    Send a message to RabbitMQ.
    
    Parameters:
    message (dict): The message to send to RabbitMQ.
    queue_name (str): The RabbitMQ queue name to send the message to.
    """
    try:
        # Establish connection to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_PARAMS['host']))
        channel = connection.channel()

        # Declare the queue in case it doesn't exist
        channel.queue_declare(queue=queue_name)

        # Send the message to the queue
        channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(message))
        logging.info(f"Message sent to RabbitMQ queue '{queue_name}': {message}")

        connection.close()
    except Exception as e:
        logging.error(f"Failed to send message to RabbitMQ: {e}")

def map_hospitals():
    """Fetch hospital mapping data from an API and insert it into a PostgreSQL table."""
    logging.info('Fetching Hospitals data...')

    # API endpoint and headers for authentication
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/reporting-units-downloads/mappings"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  # No actual token required
        'User-Agent': 'MyApp/1.0',
        'accept': 'application/json'
    }

    try:
        # Send a GET request to the API
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error if the request fails
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return

    # Save the API response content to an Excel file
    filename = 'hospital_mapping.xlsx'
    with open(filename, 'wb') as file:
        file.write(response.content)

    # Load the Excel file into a Pandas DataFrame
    df = pd.read_excel(filename, engine='openpyxl', skiprows=3)

    # Rename columns to match database schema
    df.rename(columns={
        "Open/Closed": "open_closed",
        "Local Hospital Network (LHN)": "lhn",
        "Primary Health Network area (PHN)": "phn"
    }, inplace=True)

    # Convert all column names to lowercase for consistency
    df.columns = [col.lower() for col in df.columns]

    try:
        # Establish connection to PostgreSQL
        connection = psycopg2.connect(
            host=POSTGRES_PARAMS['host'],
            port=POSTGRES_PARAMS['port'],
            database=POSTGRES_PARAMS['database'],
            user=POSTGRES_PARAMS['user'],
            password=POSTGRES_PARAMS['password']
        )
        cursor = connection.cursor()

        # Insert the DataFrame data into the PostgreSQL 'hospitals' table
        logging.info("Inserting hospital mapping data into PostgreSQL...")

        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO hospitals (code, name, type, latitude, longitude, sector, open_closed, state, lhn, phn)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code) DO NOTHING;  -- Prevents duplicate insertion if 'code' already exists
            """
            values = (
                row['code'],                # Assuming 'code' column exists in your DataFrame
                row['name'],        # Assuming 'hospital_name' column exists in your DataFrame
                row['type'],                 # Assuming 'type' column exists in your DataFrame
                row['latitude'],             # Assuming 'latitude' column exists in your DataFrame
                row['longitude'],            # Assuming 'longitude' column exists in your DataFrame
                row['sector'],               # Assuming 'sector' column exists in your DataFrame
                row['open_closed'],          # Assuming 'open_closed' column exists in your DataFrame
                row['state'],                # Assuming 'state' column exists in your DataFrame
                row['lhn'],                  # Assuming 'lhn' column exists in your DataFrame
                row['phn']                   # Assuming 'phn' column exists in your DataFrame
            )
            cursor.execute(insert_query, values)

        # Commit the transaction to make changes persistent in the database
        connection.commit()

        logging.info("Hospital mapping data successfully inserted into PostgreSQL.")

    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        if connection:
            connection.rollback()  # Rollback in case of error

    finally:
        # Close the cursor and the connection, ensuring cleanup happens
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def download_datasetlist():
    """Download a list of datasets from an API, process it, and insert relevant data into PostgreSQL."""
    
    # API endpoint and headers for authentication
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  # A personal token is not actually required, you can leave this as it is
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    try:
        # Send a GET request to download the dataset list as a CSV
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            file_path = 'datasets.csv'
            # Save the CSV content to a file
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logging.info("List of available datasets retrieved")

            # Load the CSV file into a Pandas DataFrame
            df = pd.read_csv(file_path)

            # Convert all column names to lowercase
            df.columns = [col.lower() for col in df.columns]
            
            # Process specific columns and remove duplicates
            reported_measurements = df[['reportedmeasurecode', 'reportedmeasurename']].drop_duplicates()
            measurements = df[['measurecode', 'measurename']].drop_duplicates()
            values = df[['reportingstartdate', 'reportedmeasurecode', 'datasetid', 'measurecode', 'datasetname']]

            # Convert the 'reportingstartdate' column to datetime
            values.loc[:, 'reportingstartdate'] = pd.to_datetime(values['reportingstartdate'], format='%Y-%m-%d', errors='coerce')


            # Insert processed data into PostgreSQL tables directly
            connection = psycopg2.connect(
                host=POSTGRES_PARAMS['host'],
                port=POSTGRES_PARAMS['port'],
                database=POSTGRES_PARAMS['database'],
                user=POSTGRES_PARAMS['user'],
                password=POSTGRES_PARAMS['password']
            )
            cursor = connection.cursor()

            # Insert data into reported_measurements table
            logging.info("Inserting data into 'reported_measurements' table...")
            for _, row in reported_measurements.iterrows():
                insert_query = """
                INSERT INTO reported_measurements (reportedmeasurecode, reportedmeasurename)
                VALUES (%s, %s)
                """
                cursor.execute(insert_query, (row['reportedmeasurecode'], row['reportedmeasurename']))
            
            # Insert data into measurements table
            logging.info("Inserting data into 'measurements' table...")
            for _, row in measurements.iterrows():
                insert_query = """
                INSERT INTO measurements (measurecode, measurename)
                VALUES (%s, %s)
                """
                cursor.execute(insert_query, (row['measurecode'], row['measurename']))

            # Insert data into datasets table with stored = FALSE
            logging.info("Inserting data into 'datasets' table...")
            for _, row in values.iterrows():
                insert_query = """
                INSERT INTO datasets (reportingstartdate, reportedmeasurecode, datasetid, measurecode, datasetname, stored)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    row['reportingstartdate'], 
                    row['reportedmeasurecode'], 
                    row['datasetid'], 
                    row['measurecode'], 
                    row['datasetname'],
                    False  # Set 'stored' column to FALSE initially
                ))

            # Commit the transactions
            connection.commit()

            # Close the cursor and connection
            cursor.close()
            connection.close()

            logging.info("Data successfully inserted into PostgreSQL tables.")

        else:
            # Log an error if the API request fails
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            return None

    except Exception as e:
        # Log an error if an exception occurs during the data download process
        logging.error(f"Exception occurred while fetching datasets list: {e}")
        return None

def get_ids():
    """Fetch all DataSetIds from the 'datasets' table where 'stored' is False."""
    
    # Connection details for PostgreSQL
    conn_details = {
        "host": "postgres",
        "dbname": "mydatabase",
        "user": "myuser",
        "password": "mypassword"
    }

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**conn_details)
    cursor = conn.cursor()

    # SQL query to select DataSetIds where 'stored' is False
    sql = "SELECT DataSetId FROM datasets WHERE stored = FALSE;"

    try:
        # Execute the query
        cursor.execute(sql)
        # Fetch all rows from the query result
        rows = cursor.fetchall()
        # Extract DataSetIds from the rows
        dataset_ids = [row[0] for row in rows]
        return dataset_ids
    except Exception as e:
        # Print an error message if the query fails
        print(f"An error occurred: {e}")
        return []  # Return an empty list in case of error
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

def update_total_records(dataset_id, total_records):
    try:
        connection = psycopg2.connect(**POSTGRES_PARAMS)
        cursor = connection.cursor()
        update_query = "UPDATE datasets SET totalrecords = %s WHERE datasetid = %s"
        cursor.execute(update_query, (total_records, dataset_id))
        connection.commit()
    except Exception as e:
        logging.error(f"Failed to update totalrecords for dataset {dataset_id}: {e}")
    finally:
        cursor.close()
        connection.close()

def fetch_values(dataset_id):
    """
    Fetch data items for a given dataset ID from a remote API and send each record with necessary fields to RabbitMQ.
    
    Parameters:
    dataset_id (int): ID of the dataset to fetch data for.
    """

    # Base URL for the API endpoint
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    
    # Headers for the API request
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  # No token is actually required
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'  # Expecting CSV response
    }

    # Construct the URL for the specific dataset
    url = f"{base_url}{dataset_id}/data-items"
    try:
        # Send GET request to the API
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
        # Process CSV data
            csv_data = StringIO(response.text)
            csv_reader = csv.DictReader(csv_data)
            rows = list(csv_reader)  # Convert to list to count total records
            total_records = len(rows)

            # Update the total_records in the datasets table
            update_total_records(dataset_id, total_records)

            # Loop through each row and send to RabbitMQ
            for row in rows:
                # Extract necessary fields
                record = {
                    'datasetid': dataset_id,
                    'reportingunitcode': row.get('ReportingUnitCode'),
                    'value': row.get('Value'),
                    'caveats': row.get('Caveats')
                }
                send_to_rabbitmq(record, RABBITMQ_PARAMS['queue'])
        else:
            logging.error(f"Failed to fetch dataset {dataset_id}. Status code: {response.status_code}")

    except Exception as e:
        logging.error(f"Exception occurred while fetching dataset {dataset_id}: {e}")


if __name__ == "__main__":
    # Run the hospital mapping first
    map_hospitals()

    download_datasetlist()

    dataset_ids = get_ids()

    for id in dataset_ids[-1:0:-1]:

        fetch_values(id)
