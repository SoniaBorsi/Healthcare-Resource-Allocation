import logging
import pandas as pd
import pika
import time
from tqdm import tqdm
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
import psycopg2


def update_stored(batch):
    conn_details = {
        "host": "postgres",
        "dbname": "mydatabase",
        "user": "myuser",
        "password": "mypassword"
    }

    conn = psycopg2.connect(**conn_details)
    cursor = conn.cursor()

    # Prepare the SQL query
    # This uses a parameterized query to avoid SQL injection risks
    sql = "UPDATE datasets SET stored = TRUE WHERE DataSetId = ANY(%s);"

    try:
        # Execute the SQL command
        cursor.execute(sql, (batch,))
        conn.commit()  # Commit the changes to the database
        print(f"Updated {cursor.rowcount} rows successfully.")
    except Exception as e:
        # Handle exceptions and rollback changes in case of error
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection to release database resources
        cursor.close()
        conn.close()

def map_hospitals(spark_session):
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

    sdf = spark_session.createDataFrame(df)   
    sdf = sdf.withColumnRenamed("Open/Closed", "Open_Closed") \
               .withColumnRenamed("Local Hospital Network (LHN)", "LHN") \
               .withColumnRenamed("Primary Health Network area (PHN)", "PHN")
    
    insert_into_postgresql(spark_session, sdf, "hospitals")
    print("Hospital mapping inserted successfully into the PostgreSQL database")

def get_ids():
    """Fetches all DataSetIds from the datasets table where stored is False."""
    
    # Connection details (replace with your database specifics)
    conn_details = {
        "host": "postgres",
        "dbname": "mydatabase",
        "user": "myuser",
        "password": "mypassword"
    }

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**conn_details)
    cursor = conn.cursor()

    # SQL query to select DataSetIds where stored is False
    sql = "SELECT DataSetId FROM datasets WHERE stored = FALSE;"

    try:
        # Execute the query
        cursor.execute(sql)
        # Fetch all rows of the result
        rows = cursor.fetchall()
        # Extract DataSetIds from the rows
        dataset_ids = [row[0] for row in rows]
        return dataset_ids
    except Exception as e:
        print(f"An error occurred: {e}")
        return []  # Return an empty list in case of error
    finally:
        # Close the cursor and the connection
        cursor.close()
        conn.close()



def insert_into_postgresql(spark,data_frame, table_name):
    url = "jdbc:postgresql://postgres:5432/mydatabase"
    properties = {
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }

    ids ={"hospitals" : "code",
          "measurements" : "measuerecode",
          "reportedmeasurements" : "reportedmeasurecode",
          "datasets" : "datasetid"}

    # Read existing data from the table
    try:
        existing_df = spark.read.format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .load()

        # Assuming 'id' is the primary key column in your DataFrame and the PostgreSQL table
        if ids[table_name] in data_frame.columns:
            # Perform a left anti join to find new records
            new_records_df = data_frame.join(existing_df, data_frame[ids[table_name]] == existing_df[ids[table_name]], "left_anti")
            
            if new_records_df.count() > 0:
                # Insert new unique records
                new_records_df.write.format("jdbc") \
                    .option("url", url) \
                    .option("dbtable", table_name) \
                    .option("user", properties["user"]) \
                    .option("password", properties["password"]) \
                    .option("driver", properties["driver"]) \
                    .mode("append") \
                    .save()
                logging.info(f"Data successfully inserted into {table_name}.")
            else:
                logging.info("No new unique records to insert.")
        else:
            logging.error("Primary key not in DataFrame columns.")

    except Exception as e:
        logging.error(f"Failed to interact with PostgreSQL: {e}")
        raise

def send_to_rabbitmq(concatenated_csv):
    connection_attempts = 0
    max_attempts = 5
    while connection_attempts < max_attempts:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()
            channel.queue_declare(queue='values_queue')
            channel.basic_publish(exchange='', routing_key='values_queue', body=concatenated_csv)
        
            logging.info("Data sent to RabbitMQ.")
            connection.close()
            return
        except Exception as e:
            logging.error(f"Failed to send CSV files to RabbitMQ: {e}")
            connection_attempts += 1
            time.sleep(5)
    logging.error("Exceeded maximum attempts to connect to RabbitMQ.")
    
def consume_from_rabbitmq(spark_session, queue_name, callback_function):
    try:
        # Setup the connection to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        
        # Make sure the queue exists
        channel.queue_declare(queue=queue_name, passive=True)

        # Define the callback function that handles incoming messages
        def on_message_callback(ch, method, properties, body):
            callback_function(spark_session, ch, method, properties, body)
        
        # Set up basic consume
        channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback, auto_ack=True)
        
        # Start consuming messages from the queue
        logging.info(f'[*] Waiting for messages on queue "{queue_name}". To exit press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        # Handle KeyboardInterrupt gracefully
        logging.info("KeyboardInterrupt detected. Stopping consumption.")
        channel.stop_consuming()
    except Exception as e:
        # Log any exceptions that occur
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")
    finally:
        # Always close the connection when done or if an error occurs
        if 'connection' in locals():
            connection.close()

def download_datasetlist(spark_session):
    url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            file_path = 'datasets.csv'
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logging.info("List of available data retrieved")

            df = pd.read_csv(file_path)
            
            sdf = spark_session.createDataFrame(df)
        
            reportedmeasurements = sdf.select('ReportedMeasureCode', 'ReportedMeasureName').dropDuplicates()
            measurements = sdf.select('MeasureCode', 'MeasureName').dropDuplicates()
            values = sdf.select('ReportingStartDate', 'ReportedMeasureCode', 'DataSetId', 'MeasureCode', 'DatasetName')
            values = values.withColumn("ReportingStartDate", to_date(col("ReportingStartDate"), "yyyy-MM-dd"))
        
            insert_into_postgresql(spark_session, reportedmeasurements, "reported_measurements")
            insert_into_postgresql(spark_session, measurements, "measurements")
            insert_into_postgresql(spark_session, values, "datasets")

        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching datasets list: {e}")
        return None
