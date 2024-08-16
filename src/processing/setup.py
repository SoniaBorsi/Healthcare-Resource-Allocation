from pyspark.sql import SparkSession
import utilities.tables 

utilities.tables.schema()

spark = SparkSession.builder \
    .appName("Healthcare-Resource-Allocation") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/processing/jars/postgresql-42.7.3.jar") \
    .getOrCreate()