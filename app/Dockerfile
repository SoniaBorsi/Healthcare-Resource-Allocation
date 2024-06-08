FROM bitnami/spark:latest

COPY datasets.py /opt/bitnami/spark/datasets.py

COPY postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

RUN pip install pika py4j requests

CMD ["python", "/opt/bitnami/spark/datasets.py"]
