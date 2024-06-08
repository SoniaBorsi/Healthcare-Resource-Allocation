import logging
import tempfile
from pyspark.sql import SparkSession
import requests
import pika
import time

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Healthcare-Resource-Allocation") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark

def download_datasets_csv():
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
            logging.info("CSV file downloaded successfully and saved as 'datasets.csv'.")
            return [file_path]  # Return as a list to be compatible with send_to_rabbitmq
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching datasets list: {e}")
        return None

def send_to_rabbitmq(csv_files):
    connection_attempts = 0
    max_attempts = 5
    while connection_attempts < max_attempts:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()
            channel.queue_declare(queue='datasets_measurements_reportedmeasurements_queue')

            for csv_file in csv_files:
                with open(csv_file, 'r') as file:
                    csv_data = file.read()
                    channel.basic_publish(exchange='', routing_key='datasets_measurements_reportedmeasurements_queue', body=csv_data)

            connection.close()
            logging.info("CSV files sent to RabbitMQ.")
            return
        except Exception as e:
            logging.error(f"Failed to send CSV files to RabbitMQ: {e}")
            connection_attempts += 1
            time.sleep(5)
    logging.error("Exceeded maximum attempts to connect to RabbitMQ.")

def insert_into_postgresql(df, table_name):
    # Database connection parameters
    url = "jdbc:postgresql://postgres:5432/mydatabase"
    properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(url=url, table=table_name, mode='append', properties=properties)
    logging.info(f"Data inserted into {table_name} successfully.")

def callback(ch, method, properties, body):
    try:
        csv_data = body.decode('utf-8')

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        spark = create_spark_session()
        sdf = spark.read.csv(temp_file_path, header=True, inferSchema=True)
        
        reportedmeasurements = sdf.select('ReportedMeasureCode', 'ReportedMeasureName')
        measurements = sdf.select('MeasureCode', 'MeasureName')
        values = sdf.select('ReportingStartDate', 'ReportedMeasureCode', 'DataSetId', 'MeasureCode', 'DatasetName')
        
        insert_into_postgresql(reportedmeasurements, "reported_measurements")
        insert_into_postgresql(measurements, "measurements")
        insert_into_postgresql(values, "datasets")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Message processed and acknowledged.")
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

def consume_from_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='datasets_measurements_reportedmeasurements_queue')
        channel.basic_consume(queue='datasets_measurements_reportedmeasurements_queue', on_message_callback=callback)
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
    except Exception as e:
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")

def main():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    csv_files = download_datasets_csv()
    if csv_files:
        logging.info("Successfully downloaded CSV files.")
        send_to_rabbitmq(csv_files)
    else:
        logging.error("No CSV files downloaded, skipping sending to RabbitMQ.")
        return
    
    consume_from_rabbitmq()

if __name__ == "__main__":
    main()
