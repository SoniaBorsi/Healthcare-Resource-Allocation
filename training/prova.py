from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import logging


# def create_spark_session():
#     spark = SparkSession.builder \
#         .appName("Healthcare-Resource-Allocation") \
#         .config("spark.driver.extraClassPath", "/Users/soniaborsi/Desktop/postgresql-42.7.3.jar") \
#         .getOrCreate()
#     logging.info("Spark session created successfully.")
#     return spark

def read_from_postgresql(spark, table_name):
    url = "jdbc:postgresql://localhost:5432/mydatabase"
    properties = {
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }
    try:
        logging.info(f"Attempting to read table '{table_name}' from PostgreSQL.")
        df = spark.read.jdbc(url=url, table=table_name, properties=properties)
        logging.info(f"Successfully read table '{table_name}' from PostgreSQL.")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from PostgreSQL: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        spark = create_spark_session()
        df = read_from_postgresql(spark, "values")
        logging.info("DataFrame schema:")
        df.printSchema()
    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}")

# def preprocess_data(df):
#     indexers = [StringIndexer(inputCol=column, outputCol=column+"_indexed").fit(df) 
#                 for column in ['feature1', 'feature2']]

#     assembler = VectorAssembler(inputCols=['feature1_indexed', 'feature2_indexed'], outputCol="features")

#     pipeline = Pipeline(stages=indexers + [assembler])
#     model = pipeline.fit(df)
#     df = model.transform(df)

#     return df.select("features", "label")

# def train_model(df):

#     train_df, test_df = df.randomSplit([0.8, 0.2], seed=1234)

#     lr = LogisticRegression(featuresCol='features', labelCol='label')
#     lr_model = lr.fit(train_df)

#     predictions = lr_model.transform(test_df)

#     evaluator = BinaryClassificationEvaluator(labelCol="label")
#     accuracy = evaluator.evaluate(predictions)

#     lr_model.save("path/to/save/model")

#     return lr_model
