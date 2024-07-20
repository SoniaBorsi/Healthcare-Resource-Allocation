import logging
import requests
from tqdm import tqdm
from utils.tools import insert_into_postgresql
import io

def get_values(dataset_ids):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',  # Make sure to replace YOUR_ACCESS_TOKEN with your actual token
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    concatenated_csv = ""
    for dataset_id in dataset_ids:
        url = f"{base_url}{dataset_id}/data-items"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                concatenated_csv += response.text
            else:
                logging.error(f"Failed to fetch dataset {dataset_id}. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception occurred while fetching dataset {dataset_id}: {e}")

    return concatenated_csv



def callback_values(spark_session, ch, method, properties, body):
    try:
        csv_data = body.decode('utf-8')
        
        csv_stream = io.StringIO(csv_data)
        csv_lines = csv_stream.getvalue().split("\n")
        
        csv_rdd = spark_session.sparkContext.parallelize(csv_lines)

        sdf = spark_session.read.csv(csv_rdd, header=True, inferSchema = True)
        values = sdf.select('DataSetId', 'ReportingUnitCode', 'Value', 'Caveats')

        insert_into_postgresql(spark_session, values, 'values')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logging.error(f"Failed to process message: {e}")
