from pyspark.sql import SparkSession
import utils.values as values
import logging
import utils.tools as tools
import utils.tables 
from tqdm import tqdm

utils.tables.schema()

#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

spark = SparkSession.builder \
    .appName("Healthcare-Resource-Allocation") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

tools.map_hospitals(spark)
datasets_csv = tools.download_datasetlist(spark)

datasets_ids = tools.get_ids()
batches = [datasets_ids[i:i+20] for i in range(0, len(datasets_ids), 20)][:5]

for batch in tqdm(batches, desc='Fetching data ...'):
    values_csv = values.get_values(batch)
    if values_csv:
        logging.info("Processing batch...")
        tools.send_to_rabbitmq(values_csv)
        tools.consume_from_rabbitmq(spark, "values_queue", values.callback_values)
        tools.update_stored(batch)