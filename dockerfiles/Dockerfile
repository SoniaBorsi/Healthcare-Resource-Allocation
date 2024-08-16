FROM bitnami/spark:3.5

USER root

RUN mkdir -p /.cache && chmod -R 777 /.cache

RUN mkdir /opt/bitnami/spark/processing

COPY src/processing /opt/bitnami/spark/processing

RUN pip install -r opt/bitnami/spark/processing/requirements_spark.txt

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]