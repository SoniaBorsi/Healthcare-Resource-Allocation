FROM bitnami/spark:3.5

USER root

# Create cache directory with appropriate permissions
RUN mkdir -p /.cache && chmod -R 777 /.cache

# Create directory for processing scripts
RUN mkdir -p /opt/bitnami/spark/processing

# Copy processing scripts to the created directory
COPY src/processing/ /opt/bitnami/spark/processing

# Install required Python packages
RUN pip install --no-cache-dir -r /opt/bitnami/spark/processing/requirements_spark.txt

# Set the command to start the Spark master
CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]

RUN python3 /opt/bitnami/spark/processing/setup.py
