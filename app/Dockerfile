FROM bitnami/spark:3.5

USER root

RUN mkdir -p /.cache && chmod -R 777 /.cache

COPY requirements.txt opt/bitnami/spark/requirements.txtì

RUN pip install -r opt/bitnami/spark/requirements.txt

COPY ETL.py /opt/bitnami/spark/ETL.py

COPY postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

RUN mkdir  /opt/bitnami/spark/utils 

COPY utils /opt/bitnami/spark/utils/

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]