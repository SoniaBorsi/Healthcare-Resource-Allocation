import logging
import tempfile
import requests
import pika
from utils.tools import insert_into_postgresql

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


def callback_datasets(spark_session, ch, method, properties, body):
    try:
        csv_data = body.decode('utf-8')

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        sdf = spark_session.read.csv(temp_file_path, header=True, inferSchema=True)
        
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

def consume_from_rabbitmq_datasets(spark_session):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='datasets_measurements_reportedmeasurements_queue')
        channel.basic_consume(queue='datasets_measurements_reportedmeasurements_queue', on_message_callback=lambda ch, method, properties, body: callback_datasets(spark_session, ch, method, properties, body))
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
    except Exception as e:
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")