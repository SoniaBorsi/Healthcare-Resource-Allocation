import logging
import tempfile
import requests
import pika
from utils.tools import insert_into_postgresql


def get_values(dataset_ids):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    csv_files = []
    for dataset_id in dataset_ids[:10]:
        url = f"{base_url}{dataset_id}/data-items"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    temp_file.write(response.text)
                    temp_file_path = temp_file.name
                    csv_files.append(temp_file_path)
            else:
                logging.error(f"Failed to fetch dataset {dataset_id}. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception occurred while fetching dataset {dataset_id}: {e}")
    
    return csv_files


def callback_values(spark_session, ch, method, properties, body, spark):
    try:
        csv_data = body.decode('utf-8')
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        sdf = spark_session.read.csv(temp_file_path, header=True, inferSchema=True)
        values = sdf.select('DataSetId', 'ReportingUnitCode', 'Value', 'Caveats')

        insert_into_postgresql(values, 'values')

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Message processed and acknowledged.")
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

def consume_from_rabbitmq_values(spark):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='values_queue')
        on_message_callback = lambda ch, method, properties, body: callback_values(ch, method, properties, body, spark)
        channel.basic_consume(queue='values_queue', on_message_callback=on_message_callback)
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info('Interrupted by user, shutting down...')
    except Exception as e:
        logging.error(f"Failed to consume messages from RabbitMQ: {e}")

        