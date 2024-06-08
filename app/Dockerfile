FROM bitnami/spark:latest

COPY  datasets.py /opt/bitnamy/spark/datasets.py

COPY postgresql-42.7.3.jar /opt/bitnamy/spark/jars/postgresql-42.7.3.jar

RUN pip install pika py4j requests

CMD ["python", "/opt/bitnamy/spark/datasets.py"]
