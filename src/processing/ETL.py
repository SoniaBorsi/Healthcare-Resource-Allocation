import utilities.tables 
import utilities.values as values
import logging
import utilities.tools as tools
from tqdm import tqdm
from setup import spark

utilities.tables.schema()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

tools.map_hospitals(spark)
datasets_csv = tools.download_datasetlist(spark)

datasets_ids = tools.get_ids()
batches = [datasets_ids[i:i+20] for i in range(0, len(datasets_ids), 20)]

for batch in tqdm(batches, desc='Fetching data ...'):
    values_csv = values.get_values(batch)
    if values_csv:
        logging.info("Processing batch...")
        tools.send_to_rabbitmq(values_csv)
        tools.consume_from_rabbitmq(spark, "values_queue", values.callback_values)
        tools.update_stored(batch)