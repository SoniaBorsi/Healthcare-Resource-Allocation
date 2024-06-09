from pyspark.sql import SparkSession
import utils.datasets as datasets
import utils.values as values
import logging
import utils.tools as tools

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

spark = SparkSession.builder \
    .appName("Healthcare-Resource-Allocation") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

tools.map_hospitals()

datasets_csv = datasets.download_datasets_csv()

datasets_ids = tools.get_ids(datasets_csv[0])
values_csv = values.get_values(datasets_ids)

if datasets_csv:
    logging.info("Successfully retrieved datasets information, sending to RabbitMQ")
    tools.send_to_rabbitmq(datasets_csv)
else:
    logging.error("No CSV files downloaded, skipping sending to RabbitMQ.")

if values_csv:
    logging.info("Successfully retrieved values, sending to RabbitMQ")
    tools.send_to_rabbitmq(values_csv)

tools.consume_from_rabbitmq(spark, "datasets_measurements_reportedmeasurements_queue", datasets.callback_datasets)
tools.consume_from_rabbitmq(spark, "values_queue", values.callback_values)