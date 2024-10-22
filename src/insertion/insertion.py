import pika
import psycopg2
import logging
import json
import time

# PostgreSQL connection parameters
POSTGRES_PARAMS = {
    "host": "postgres",  # Use the Docker service name if running in Docker
    "port": 5432,
    "database": "mydatabase",
    "user": "myuser",
    "password": "mypassword"
}

# RabbitMQ connection parameters
RABBITMQ_PARAMS = {
    "host": "rabbitmq",  # Use the Docker service name if running in Docker
    "queue": "values"
}

# Global dictionaries to keep track of counts per dataset
processed_counts = {}           # In-memory count of processed records not yet committed to DB
database_processed_counts = {}  # Count of records already committed to the database
total_records = {}              # Total number of records per dataset

def insert_into_postgresql_values(record):
    """
    Insert a record into the PostgreSQL database and update processed counts.
    """
    try:
        # Establish connection to PostgreSQL
        connection = psycopg2.connect(**POSTGRES_PARAMS)
        cursor = connection.cursor()

        # Prepare the SQL query
        insert_query = """
        INSERT INTO info (datasetid, reportingunitcode, value, caveats, id)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Convert 'value' to a numeric type or set to None
        value = record['value']
        if value == "" or value is None:
            value = None
        else:
            try:
                value = float(value)
            except ValueError:
                value = None

        # Extract 'CaveatName' from 'caveats'
        caveats = record.get('caveats', "")
        caveat_name = None
        if caveats:
            try:
                caveat_list = json.loads(caveats)
                if isinstance(caveat_list, list) and len(caveat_list) > 0:
                    caveat_name = caveat_list[0].get('CaveatName')
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode caveats JSON: {e}")

        # Generate 'id' by concatenating 'datasetid' and 'reportingunitcode'
        record_id = f"{record['datasetid']}{record['reportingunitcode']}"

        # Values to insert
        values = (
            record['datasetid'],
            record['reportingunitcode'],
            value,
            caveat_name,
            record_id
        )

        # Execute the insert query
        cursor.execute(insert_query, values)
        connection.commit()

        cursor.close()
        connection.close()
        logging.info(f"Successfully inserted record with ID: {record_id}")

        # Update processed counts
        dataset_id = record['datasetid']

        # Initialize counts if necessary
        if dataset_id not in processed_counts:
            processed_counts[dataset_id] = 0
            database_processed_counts[dataset_id] = 0
            total_records[dataset_id] = get_total_records(dataset_id)

        # Increment in-memory processed count
        processed_counts[dataset_id] += 1

        # Total processed records
        total_processed = database_processed_counts[dataset_id] + processed_counts[dataset_id]

        # Check if it's time to update the database
        if processed_counts[dataset_id] >= 100 or total_processed == total_records[dataset_id]:
            # Update the 'processed_records' in the database
            update_processed_records(dataset_id, processed_counts[dataset_id])

            # Update the database_processed_counts
            database_processed_counts[dataset_id] += processed_counts[dataset_id]

            # Reset the in-memory processed count
            processed_counts[dataset_id] = 0

            # Log progress
            percentage = calculate_percentage(dataset_id)
            logging.info(f"Dataset {dataset_id}: {percentage:.2f}% of data stored.")

            # If dataset is fully processed, update 'stored' to TRUE
            if total_processed == total_records[dataset_id]:
                set_dataset_stored(dataset_id, True)
                logging.info(f"Dataset {dataset_id} fully stored.")

    except Exception as e:
        logging.error(f"Failed to insert record into PostgreSQL: {e}")

def get_total_records(dataset_id):
    """
    Fetch 'total_records' for a 'dataset_id' from the 'datasets' table.
    """
    try:
        connection = psycopg2.connect(**POSTGRES_PARAMS)
        cursor = connection.cursor()
        query = "SELECT totalrecords FROM datasets WHERE datasetid = %s"
        cursor.execute(query, (dataset_id,))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        if result:
            return result[0]
        else:
            logging.error(f"No total_records found for dataset_id {dataset_id}")
            return 0
    except Exception as e:
        logging.error(f"Failed to get total_records for dataset {dataset_id}: {e}")
        return 0

def update_processed_records(dataset_id, count):
    """
    Update the 'processed_records' count in the 'datasets' table by 'count'.
    """
    try:
        connection = psycopg2.connect(**POSTGRES_PARAMS)
        cursor = connection.cursor()
        update_query = """
        UPDATE datasets
        SET processedrecords = processedrecords + %s
        WHERE datasetid = %s
        """
        cursor.execute(update_query, (count, dataset_id))
        connection.commit()
        cursor.close()
        connection.close()
        logging.info(f"Updated processed_records for dataset {dataset_id} by {count}")
    except Exception as e:
        logging.error(f"Failed to update processed_records for dataset {dataset_id}: {e}")

def set_dataset_stored(dataset_id, stored):
    """
    Update the 'stored' column in the 'datasets' table for the 'dataset_id'.
    """
    try:
        connection = psycopg2.connect(**POSTGRES_PARAMS)
        cursor = connection.cursor()
        update_query = "UPDATE datasets SET stored = %s WHERE datasetid = %s"
        cursor.execute(update_query, (stored, dataset_id))
        connection.commit()
        cursor.close()
        connection.close()
        logging.info(f"Set stored = {stored} for dataset {dataset_id}")
    except Exception as e:
        logging.error(f"Failed to set stored = {stored} for dataset {dataset_id}: {e}")

def calculate_percentage(dataset_id):
    """
    Calculate and return the percentage of data stored for a given dataset.
    """
    try:
        connection = psycopg2.connect(**POSTGRES_PARAMS)
        cursor = connection.cursor()
        query = "SELECT processedrecords, totalrecords FROM datasets WHERE datasetid = %s"
        cursor.execute(query, (dataset_id,))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        if result and result[1] > 0:
            processed_records_db, total_records_db = result
            # Include in-memory counts
            in_memory_count = processed_counts.get(dataset_id, 0)
            total_processed = processed_records_db + in_memory_count
            percentage = (total_processed / total_records_db) * 100
            return percentage
        else:
            return 0.0
    except Exception as e:
        logging.error(f"Failed to calculate percentage for dataset {dataset_id}: {e}")
        return 0.0

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

        # Map the parsed values to the expected record fields
        record = {
            'datasetid': message_dict['datasetid'],
            'reportingunitcode': message_dict['reportingunitcode'],
            'value': message_dict['value'],
            'caveats': message_dict['caveats']
        }

        # Insert the record into PostgreSQL
        insert_into_postgresql_values(record)

        # Acknowledge the message in RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logging.error(f"Failed to process message: {e}")
        # Optionally, reject the message or requeue it
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

MAX_RETRIES = 10
RETRY_DELAY = 5  # seconds

def start_rabbitmq_consumer():
    """
    Set up RabbitMQ consumer to listen to the queue and process messages with retry mechanism.
    """
    retries = 0

    while retries < MAX_RETRIES:
        try:
            # Establish connection to RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_PARAMS['host'])
            )
            channel = connection.channel()

            # Declare the queue
            channel.queue_declare(queue=RABBITMQ_PARAMS['queue'])

            # Start consuming messages from the queue
            channel.basic_consume(
                queue=RABBITMQ_PARAMS['queue'],
                on_message_callback=callback
            )
            logging.info(f"Waiting for messages in queue '{RABBITMQ_PARAMS['queue']}'...")

            # Start consuming (this will block and keep waiting for messages)
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
