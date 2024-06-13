FROM bitnami/spark:3.5

COPY main.py /opt/bitnami/spark/main.py

COPY postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

RUN mkdir  /opt/bitnami/spark/utils 

COPY utils /opt/bitnami/spark/utils/

RUN pip install pika py4j requests pandas plotly streamlit_folium streamlit_chat psycopg2-binary openpyxl tqdm streamlit

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]