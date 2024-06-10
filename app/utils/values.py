import logging
import tempfile
import requests
from tqdm import tqdm
from utils.tools import insert_into_postgresql


def get_values(dataset_ids):
    base_url = "https://myhospitalsapi.aihw.gov.au/api/v1/datasets/"
    headers = {
        'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'MyApp/1.0',
        'accept': 'text/csv'
    }

    csv_files = []
    with tqdm(total=len(dataset_ids), desc='Fetching datasets') as pbar:
        for dataset_id in dataset_ids:
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
            finally:
                pbar.update(1)
    
    return csv_files



def callback_values(spark_session, ch, method, properties, body):
    try:
        csv_data = body.decode('utf-8')
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name
        
        sdf = spark_session.read.csv(temp_file_path, header=True, inferSchema=True)
        values = sdf.select('DataSetId', 'ReportingUnitCode', 'Value', 'Caveats')

        insert_into_postgresql(values, 'values')

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

        