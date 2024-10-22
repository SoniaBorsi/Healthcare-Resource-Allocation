import pika
import psycopg2
import logging
import json
import io
import time
import csv 

# PostgreSQL connection parameters (use container service name for `host`)
POSTGRES_PARAMS = {
    "host": "postgres",  # Use the Docker service name for the PostgreSQL container
    "port": 5432,  # Default PostgreSQL port
    "database": "mydatabase",
    "user": "myuser",
    "password": "mypassword"
}

# RabbitMQ connection parameters
RABBITMQ_PARAMS = {
    "host": "rabbitmq",  # Use the Docker service name for the RabbitMQ container
    "queue": "values"
}


def insert_into_postgresql_values(record):
    """
    Insert a record into the PostgreSQL database hosted on the container.

    Parameters:
    record (dict): A dictionary containing the values to insert into the database.
    """
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

        # Prepare the SQL query
        insert_query = """
        INSERT INTO info (datasetid, reportingunitcode, value, caveats, id)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Convert the 'value' field to a valid numeric value or NULL if empty
        value = record['value']
        if value == "" or value is None:
            value = None  # Set to None so that PostgreSQL will treat it as NULL

        # Extract 'CaveatName' from the 'caveats' field
        caveats = record.get('caveats', [])
        caveat_name = None
        if caveats:
            try:
                caveat_list = json.loads(caveats)  # Ensure it's properly parsed as JSON
                if isinstance(caveat_list, list) and len(caveat_list) > 0:
                    caveat_name = caveat_list[0].get('CaveatName', None)  # Extract 'CaveatName'
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode caveats JSON: {e}")

        # Generate the 'id' column by concatenating 'datasetid' and 'reportingunitcode'
        record_id = str(record['datasetid']) + record['reportingunitcode']

        # Tuple containing the values to insert
        values = (
            record['datasetid'],                # Numeric dataset ID
            record['reportingunitcode'],         # Text reporting unit code
            value,                               # Handle numeric 'value', set as NULL if empty
            caveat_name,                         # Insert only the CaveatName
            record_id                            # Unique ID generated from datasetid and reportingunitcode
        )

        cursor.execute(insert_query, values)
        connection.commit()

        cursor.close()
        connection.close()
        logging.info(f"Successfully inserted record with ID: {record_id}")

    except Exception as e:
        logging.error(f"Failed to insert record into PostgreSQL: {e}")




def callback(ch, method, properties, body):
    """
    Callback function to process the message received from RabbitMQ.
    """
    try:
        # Decode the message body to a string
        message = body.decode('utf-8')
        logging.info(f"Received message: {message}")

        # Parse the message as a dictionary (assuming it's in JSON format)
        message_dict = json.loads(message)

        # Map the parsed CSV values to the expected record fields
        record = {
            'datasetid': message_dict['datasetid'],    # Use dataset_id from the main message
            'reportingunitcode': message_dict['reportingunitcode'],            # Assuming 'reportingunitcode' is the third column
            'caveats': message_dict['caveats'],                # Assuming hospital name is the fourth column
            'value': message_dict['value'],                    # Assuming latitude is the 15th column (adjust as needed)
            }

        # Insert the record into PostgreSQL
        insert_into_postgresql_values(record)

        # Acknowledge the message in RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logging.error(f"Failed to process message: {e}")


MAX_RETRIES = 5
RETRY_DELAY = 5

def start_rabbitmq_consumer():
    """
    Set up RabbitMQ consumer to listen to the queue and process messages with retry mechanism.
    """
    retries = 0

    while retries < MAX_RETRIES:
        try:
            # Establish connection to RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_PARAMS['host']))
            channel = connection.channel()

            # Declare the queue (in case it doesn't exist)
            channel.queue_declare(queue=RABBITMQ_PARAMS['queue'])

            # Start consuming messages from the queue
            channel.basic_consume(queue=RABBITMQ_PARAMS['queue'], on_message_callback=callback)
            logging.info(f"Waiting for messages in queue '{RABBITMQ_PARAMS['queue']}'...")

            # Start consuming messages (this will block and keep waiting for messages)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            retries += 1
            logging.error(f"Failed to connect to RabbitMQ (attempt {retries}/{MAX_RETRIES}): {e}")
            if retries < MAX_RETRIES:
                logging.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logging.error("Max retries reached. Exiting.")
                break  # Exit the retry loop after max retries

        except Exception as e:
            # Handle other possible exceptions
            logging.error(f"An unexpected error occurred: {e}")
            break  # Exit on any unexpected error


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Start the RabbitMQ consumer
    start_rabbitmq_consumer()
