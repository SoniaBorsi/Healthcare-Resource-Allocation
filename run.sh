SPARK_MASTER_URL=spark://spark-master:7077
PROGRAM_PATH=processing/ETL.py

docker compose up --build -d
docker compose exec spark-master spark-submit --jars processing/jars/postgresql-42.7.3.jar --master $SPARK_MASTER_URL $PROGRAM_PATH
